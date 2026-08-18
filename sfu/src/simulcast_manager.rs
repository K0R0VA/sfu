use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};


use rtc::{rtp::Packet};
use tokio::{sync::mpsc::Sender, time::timeout};
use webrtc::media_stream::track_remote::{TrackRemote, TrackRemoteEvent};

use crate::{SignalingClient, Storage, actor::{Actor, Addr}, error::Error, keyframe_interceptor::KeyframeInterceptor, publisher::{Publisher, PublisherMessage}, room::{Codec, StreamQuality, VideoRouterStream}, rtp_packet_gateway_router::{RtpPacketGatewayRouter, RtpPacketGatewayRouterMessage, VideoRouterContext}, server::Key, video_packet_forwarder::VideoPacketForwarder};

pub struct SimulcastManager<K: Key, C: SignalingClient<UserKey = K>, S: Storage> {
    pub layers: HashMap<u32, Sender<Packet>>,
    pub publisher: Addr<Publisher<K, C, S>>,
    pub track: Arc<dyn TrackRemote>,
    pub codek: Codec
}

impl<K: Key, C: SignalingClient<UserKey = K>, S: Storage> SimulcastManager<K, C, S> {
    fn new(track: Arc<dyn TrackRemote>, codek: Codec, publisher: Addr<Publisher<K, C, S>>) -> Self {
        Self {
            publisher,
            track,
            codek,
            layers: HashMap::with_capacity(3)
        }
    }
    async fn handle_new_layer(&mut self, ssrc: u32, rid: Option<String>) -> Result<(), Error> {
        let quality = StreamQuality::from_str(rid.as_deref().unwrap_or("high"))?;
        let (wake_rx, wake_tx) = tokio::sync::broadcast::channel(1);
        let wake_tx = Arc::new(wake_tx);
        let context = VideoRouterContext::new(self.track.clone(), quality, ssrc, wake_rx);   
        let router = RtpPacketGatewayRouter::new(context).start_with_capacity(1024);
        let (tx, mut rx) = tokio::sync::mpsc::channel(32); 
        self.layers.insert(ssrc, tx);
        let keyframe_interceptor = KeyframeInterceptor::new(self.track.clone(), ssrc).start();
        let video_router_stream = VideoRouterStream {
            codec: self.codek.clone(),
            keyframe_interceptor,
            router: router.clone(),
            wake_tx
        };
        self.publisher.send(PublisherMessage::NewVideoTrack { quality, video_router_stream }).await?;
        tokio::spawn(async move {
            let timeout_duration = Duration::from_millis(300);
            let mut is_already_it_timeout = false;
            loop {
                let fut = rx.recv();
                let message = match timeout(timeout_duration, fut).await {
                    Ok(Some(packet)) => { 
                        is_already_it_timeout = false;
                        RtpPacketGatewayRouterMessage::RtpPacket(packet)
                    },
                    Ok(None) => break,
                    Err(_) if is_already_it_timeout => continue,
                    Err(_) => {
                        is_already_it_timeout = true;
                        RtpPacketGatewayRouterMessage::Timeout
                    }
                };
                if router.send(message).await.is_err() {
                    break;
                }
            }
        });
        Ok(())
    }
    async fn send_packet(&mut self, mut packet: Packet) -> Result<(), Error> {
        let ssrc = packet.header.ssrc;
        packet.header.extensions.clear();
        packet.header.extension = false;
        let Some(router) = self.layers.get(&ssrc) else { return Ok(()); };
        router.send(packet).await.map_err(|_| Error::ChannelClosed)?;
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
    pub async fn spawn(track: Arc<dyn TrackRemote>, publisher: Addr<Publisher<K, C, S>>) -> Result<(), Error> {
        let ssrc = track.ssrcs().await[0];
        let mime_type = track.codec(ssrc).await.map(|c| c.mime_type).unwrap();
        let codec = Codec::from_str(&mime_type).unwrap_or_default();
        let mut this= Self::new(track, codec, publisher);
        tokio::spawn(async move {
            this.consume_events().await?;
            Result::<_, Error>::Ok(())
        });
        Ok(())
    }
}