use std::{collections::{BTreeMap}, sync::Arc, time::Duration};
use uuid::Uuid;
use webrtc::{peer_connection::RTCPeerConnection, rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::{track_local_static_rtp::TrackLocalStaticRTP}};

use crate::{PacketVideoSubscription, actor::{Actor, Addr}, error::Error, room::{MimeType, StreamQuality}, rtp_packet_forwarder::RtpPacketGatewayRouterMessage, video_packet_forwarder::{VideoPacketForwarder, VideoPacketForwarderMessage}};

pub struct VideoSubscription {
    pub pc: Arc<RTCPeerConnection>,
    pub peer_id: Uuid,
    pub connections: BTreeMap<StreamQuality, PacketVideoSubscription>,
    pub active_track: Arc<RTCRtpSender>,
    pub packet_forwarder: Addr<VideoPacketForwarder>,
    pub active_quality: StreamQuality,
    pub waiting_high_quality: bool,
}

impl VideoSubscription {
    pub async fn new(pc: Arc<RTCPeerConnection>, peer_id: Uuid, mime_type: MimeType, stream: PacketVideoSubscription, quality: StreamQuality) -> Result<Self, Error> {
        let mut connections = BTreeMap::new();
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
        let this = Self { pc, peer_id, connections, active_track, active_quality: quality, packet_forwarder, waiting_high_quality: quality == StreamQuality::Low  };
        Ok(this) 
    }
}

pub enum VideoSubscriptionMessage {
    AddSubsription { quality: StreamQuality, stream: PacketVideoSubscription },
    SwitchQualityLayer { to: StreamQuality },
    StartLowQuality,
    Drop,
    ForcePli
}


impl Actor for VideoSubscription {
    type Message = VideoSubscriptionMessage;

    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, m: Self::Message) {
        match m {
            VideoSubscriptionMessage::StartLowQuality => if self.waiting_high_quality {
                let (quality , subscription) = self.connections.iter().next().unwrap();
                self.waiting_high_quality = false;
                let _ = self.packet_forwarder.send(VideoPacketForwarderMessage::Start {
                    quality: *quality,
                    forwarder: subscription.rtp_packet_forwarder.clone()
                }).await;
            }
            VideoSubscriptionMessage::AddSubsription { quality, stream } => {
                self.connections.insert(quality, stream.clone());
                if self.waiting_high_quality && quality == StreamQuality::High {
                    self.waiting_high_quality = false;
                    self.active_quality = quality;
                    let _ = self.packet_forwarder.send(VideoPacketForwarderMessage::Start {
                        quality,
                        forwarder: stream.rtp_packet_forwarder
                    }).await;
                }
            }
            VideoSubscriptionMessage::ForcePli => if let Some(PacketVideoSubscription { rtp_packet_forwarder }) = self.connections.get(&self.active_quality) {
                let _  = rtp_packet_forwarder.send(RtpPacketGatewayRouterMessage::ForcePli).await;
            }
            VideoSubscriptionMessage::SwitchQualityLayer { to  } => {
                if let Some(PacketVideoSubscription {  rtp_packet_forwarder }) = self.connections.get(&to) {
                    let _ = self.packet_forwarder.send(VideoPacketForwarderMessage::LayerSwitched {
                        quality: to,
                        forwarder: rtp_packet_forwarder.clone()
                    }).await;
                    self.active_quality = to;
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
                        let _ = addr.send(VideoSubscriptionMessage::ForcePli).await;
                    }
                }
            }
        });
        if self.active_quality == StreamQuality::Low {
            let addr = ctx.addr.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let _ = addr.send(VideoSubscriptionMessage::StartLowQuality).await;
            });
            return;
        }
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