use std::{collections::HashMap, str::FromStr, sync::Arc};


use rtc::{rtp::Packet};
use webrtc::media_stream::track_remote::{TrackRemote, TrackRemoteEvent};

use crate::{SignalingClient, Storage, actor::{Actor, Addr}, error::Error, keyframe_interceptor::KeyframeInterceptor, publisher::{Publisher, PublisherMessage}, room::{Codec, StreamQuality, VideoRouterStream}, rtp_packet_gateway_router::{RtpPacketGatewayRouter, RtpPacketGatewayRouterMessage, VideoRouterContext}, video_packet_forwarder::VideoPacketForwarder};

pub struct SimulcastManager<C: SignalingClient, S: Storage> {
    pub layers: HashMap<u32, Addr<RtpPacketGatewayRouter<VideoPacketForwarder, VideoRouterContext>>>,
    pub publisher: Addr<Publisher<C, S>>,
    pub track: Arc<dyn TrackRemote>,
    pub codek: Codec
}

impl<C: SignalingClient, S: Storage> SimulcastManager<C, S> {
    fn new(track: Arc<dyn TrackRemote>, codek: Codec, publisher: Addr<Publisher<C, S>>) -> Self {
        Self {
            publisher,
            track,
            codek,
            layers: HashMap::with_capacity(3)
        }
    }
    async fn handle_new_layer(&mut self, ssrc: u32, rid: Option<String>) -> Result<(), Error> {
        let quality = StreamQuality::from_str(&rid.unwrap_or_default())?;
        let (wake_rx, wake_tx) = tokio::sync::broadcast::channel(1);
        let wake_tx = Arc::new(wake_tx);
        let context = VideoRouterContext::new(self.track.clone(), quality, ssrc, wake_rx);   
        let router = RtpPacketGatewayRouter::new(context).start_with_capacity(1024);
        self.layers.insert(ssrc, router.clone());
        let keyframe_interceptor = KeyframeInterceptor::new(self.track.clone(), ssrc).start();
        let video_router_stream = VideoRouterStream {
            codec: self.codek.clone(),
            keyframe_interceptor,
            router,
            wake_tx
        };
        self.publisher.send(PublisherMessage::NewVideoTrack { quality, video_router_stream }).await?;
        Ok(())
    }
    async fn send_packet(&mut self, mut packet: Packet) -> Result<(), Error> {
        let ssrc = packet.header.ssrc;
        packet.header.extensions.clear();
        let Some(router) = self.layers.get(&ssrc) else { return Ok(()); };
        router.send(RtpPacketGatewayRouterMessage::RtpPacket(packet)).await?;
        Ok(())
    }
    async fn consume_events(&mut self) -> Result<(), Error> {
        while let Some(event) = self.track.poll().await {
            match event {
                TrackRemoteEvent::OnOpen(event) => self.handle_new_layer(event.ssrc, event.rid).await?,
                TrackRemoteEvent::OnRtpPacket(packet) => self.send_packet(packet).await?,
                TrackRemoteEvent::OnEnded => break,
                _ => continue
            }
        }
        Ok(())
    }
    pub async fn spawn(track: Arc<dyn TrackRemote>, codek: Codec, publisher: Addr<Publisher<C, S>>) -> Result<(), Error> {
        let ssrc = track.ssrcs().await[0];
        let rid = track.rid(ssrc).await;
        let mut this= Self::new(track, codek, publisher);
        this.handle_new_layer(ssrc, rid).await?;
        tokio::spawn(async move {
            this.consume_events().await?;
            Result::<_, Error>::Ok(())
        });
        Ok(())
    }
}