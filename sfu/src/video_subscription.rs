use std::{collections::HashMap, sync::{Arc}};
use uuid::Uuid;
use webrtc::{peer_connection::RTCPeerConnection, rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::{track_local_static_rtp::TrackLocalStaticRTP}};

use crate::{PacketVideoSubscription, actor::{Actor, Addr}, error::Error, pli_sender::Ping, room::{MimeType, StreamQuality}, video_packet_forwarder::{VideoPacketForwarder, VideoPacketForwarderMessage}, video_subscription::VideoSubscriptionMessage::ForcePli};

pub struct VideoSubscription {
    pub pc: Arc<RTCPeerConnection>,
    pub peer_id: Uuid,
    pub connections: HashMap<StreamQuality, PacketVideoSubscription>,
    pub active_track: Arc<RTCRtpSender>,
    pub packet_forwarder: Addr<VideoPacketForwarder>,
    pub active_quality: StreamQuality,
}

impl VideoSubscription {
    pub async fn new(pc: Arc<RTCPeerConnection>, peer_id: Uuid, mime_type: MimeType, stream: PacketVideoSubscription, quality: StreamQuality) -> Result<Self, Error> {
        let mut connections = HashMap::with_capacity(3);
        let track: Arc<TrackLocalStaticRTP> = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: mime_type.to_string(),
                ..Default::default()
            },
            format!("video_{peer_id}"),
            format!("video_{peer_id}")
        ));
        let active_track = pc.add_transceiver_from_track(
                track.clone() as Arc<_>,
                Some(RTCRtpTransceiverInit { direction: RTCRtpTransceiverDirection::Sendonly, send_encodings: vec![] })
            )
            .await?
            .sender()
            .await;
        let packet_forwarder = VideoPacketForwarder::new(track.clone(), mime_type)
            .start_with_capacity(2048);
        connections.insert(quality, stream.clone());
        let this = Self { pc, peer_id, connections, active_track, active_quality: quality, packet_forwarder,   };
        Ok(this) 
    }
}

pub enum VideoSubscriptionMessage {
    AddSubsription { quality: StreamQuality, stream: PacketVideoSubscription },
    SwitchQualityLayer { to: StreamQuality },
    ForcePli,
    Drop
}


impl Actor for VideoSubscription {
    type Message = VideoSubscriptionMessage;

    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, m: Self::Message) {
        match m {
            VideoSubscriptionMessage::AddSubsription { quality, stream } => {
                self.connections.insert(quality, stream);
            }
            VideoSubscriptionMessage::ForcePli => {
                if let Some(connection) = self.connections.get(&self.active_quality) {
                    let _ = connection.pli_sender.send(Ping).await;
                }
            }
            VideoSubscriptionMessage::SwitchQualityLayer { to  } => {
                if let Some(PacketVideoSubscription {  pli_sender, rtp_packet_forwarder }) = self.connections.get(&to) {
                    let _ = self.packet_forwarder.send(VideoPacketForwarderMessage::LayerSwitched {
                        quality: to,
                        forwarder: rtp_packet_forwarder.clone()
                    }).await;
                    let _ = pli_sender.send(Ping).await;
                }
            },
            VideoSubscriptionMessage::Drop => self.stop(ctx).await,
        }
    }

    async fn starting(&mut self, ctx: &crate::actor::Ctx<'_, Self>) {
        let active_track = self.active_track.clone();
        let addr = ctx.addr.clone();
        tokio::spawn(async move {
            let mut rtcp_buf = vec![0u8; 1500];
            while let Ok((packets, _)) = active_track.read(&mut rtcp_buf).await {
                for packet in packets {
                    if packet.as_any().downcast_ref::<PictureLossIndication>().is_some() {
                        let _ = addr.send(ForcePli).await;
                    }
                }
            }
        });
        let (quality, PacketVideoSubscription {rtp_packet_forwarder, ..}) = self.connections.iter().next().unwrap();
        let _ = self.packet_forwarder.send(VideoPacketForwarderMessage::Start {
            quality: *quality,
            forwarder: rtp_packet_forwarder.clone()
        }).await;
        
        tracing::info!("[VideoSubscription] starting");
    }

    async fn stopping(mut self, _: &crate::actor::Ctx<'_, Self>) {
        self.packet_forwarder.terminate().await;
        self.connections.clear();
        if let Err(e) = self.pc.remove_track(&self.active_track).await {
            tracing::error!("[VideoSubscription] remove_track {e}");
        }
    }
}