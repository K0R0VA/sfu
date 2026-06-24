use std::{collections::{BTreeMap}, sync::Arc, time::Duration};
use uuid::Uuid;
use webrtc::{peer_connection::RTCPeerConnection, rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::{track_local_static_rtp::TrackLocalStaticRTP}};

use crate::{actor::{Actor, Addr}, error::Error, room::{MimeType, StreamQuality}, rtp_packet_forwarder::{RtpPacketGatewayRouter, RtpPacketGatewayRouterMessage}, video_packet_forwarder::{VideoPacketForwarder, VideoPacketForwarderMessage}};

pub struct VideoSubscription {
    pub pc: Arc<RTCPeerConnection>,
    pub peer_id: Uuid,
    pub quality_layers: BTreeMap<StreamQuality, Addr<RtpPacketGatewayRouter<VideoPacketForwarder>>>,
    pub active_track: Arc<RTCRtpSender>,
    pub packet_forwarder: Addr<VideoPacketForwarder>,
    pub active_quality: StreamQuality,
    pub waiting_high_quality: bool,
}

impl VideoSubscription {
    pub async fn new(pc: Arc<RTCPeerConnection>, peer_id: Uuid, mime_type: MimeType, gateway_router: Addr<RtpPacketGatewayRouter<VideoPacketForwarder>>, quality: StreamQuality) -> Result<Self, Error> {
        let mut quality_layers = BTreeMap::new();
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
        quality_layers.insert(quality, gateway_router.clone());
        let this = Self { pc, peer_id, quality_layers, active_track, active_quality: quality, packet_forwarder, waiting_high_quality: quality == StreamQuality::Low  };
        Ok(this) 
    }
}

pub enum VideoSubscriptionMessage {
    AddSubsription { quality: StreamQuality, gateway_router: Addr<RtpPacketGatewayRouter<VideoPacketForwarder>> },
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
                let Some((quality , gateway_router)) = self.quality_layers.iter().next() else { return; };
                self.waiting_high_quality = false;
                let _ = self.packet_forwarder.send(VideoPacketForwarderMessage::Start {
                    quality: *quality,
                    gateway_router: gateway_router.clone()
                }).await;
            }
            VideoSubscriptionMessage::AddSubsription { quality, gateway_router } => {
                self.quality_layers.insert(quality, gateway_router.clone());
                if self.waiting_high_quality && quality == StreamQuality::High {
                    self.waiting_high_quality = false;
                    self.active_quality = quality;
                    let _ = self.packet_forwarder.send(VideoPacketForwarderMessage::Start {
                        quality,
                        gateway_router
                    }).await;
                }
            }
            VideoSubscriptionMessage::ForcePli => if let Some(gateway_router) = self.quality_layers.get(&self.active_quality) {
                let _  = gateway_router.send(RtpPacketGatewayRouterMessage::ForcePli).await;
            }
            VideoSubscriptionMessage::SwitchQualityLayer { to  } => {
                if let Some(gateway_router) = self.quality_layers.get(&to).cloned() {
                    let _ = self.packet_forwarder.send(VideoPacketForwarderMessage::LayerSwitched {
                        quality: to,
                        gateway_router
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
        self.quality_layers.clear();
        if let Err(e) = self.pc.remove_track(&self.active_track).await {
            tracing::error!("[VideoSubscription] remove_track {e}");
        }
    }
}