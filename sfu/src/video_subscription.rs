use std::{collections::{HashMap}, sync::Arc, time::Duration};
use uuid::Uuid;
use webrtc::{peer_connection::RTCPeerConnection, rtcp::{payload_feedbacks::picture_loss_indication::PictureLossIndication, transport_feedbacks::transport_layer_nack::TransportLayerNack}, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::track_local_static_rtp::TrackLocalStaticRTP};

use crate::{actor::{Actor, Addr, StoppingExt}, error::Error, keyframe_interceptor::{KeyframeInterceptor, RequestKeyframe}, room::{MimeType, StreamQuality}, rtp_packet_gateway_router::{RtpPacketGatewayRouter}, video_packet_forwarder::{VideoPacketForwarder, VideoPacketForwarderMessage}};

pub struct VideoSubscription {
    pub pc: Arc<RTCPeerConnection>,
    pub peer_id: Uuid,
    pub quality_layers: HashMap<StreamQuality, QualityLayer>,
    pub active_track: Arc<RTCRtpSender>,
    pub packet_forwarder: Addr<VideoPacketForwarder>,
    pub active_quality: StreamQuality,
    pub waiting_high_quality: bool,
}

#[derive(Clone)]
pub struct QualityLayer {
    pub gateway_router: Addr<RtpPacketGatewayRouter<VideoPacketForwarder>>,
    pub keyframe_interceptor: Addr<KeyframeInterceptor>
}

impl VideoSubscription {
    pub async fn new(
        pc: Arc<RTCPeerConnection>, 
        peer_id: Uuid, 
        mime_type: MimeType, 
        quality: StreamQuality,
        quality_layer: QualityLayer,
    ) -> Result<Self, Error> {
        let mut quality_layers = HashMap::new();
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
        quality_layers.insert(quality, quality_layer);
        let this = Self { pc, peer_id, quality_layers, active_track, active_quality: quality, packet_forwarder, waiting_high_quality: quality != StreamQuality::High  };
        Ok(this) 
    }
}

pub enum VideoSubscriptionMessage {
    AddSubsription { quality: StreamQuality, layer: QualityLayer },
    SwitchQualityLayer { to: StreamQuality },
    StartLowQuality,
    Drop,
    ForcePli
}


impl Actor for VideoSubscription {
    type Message = VideoSubscriptionMessage;
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, m: Self::Message) {
        match m {
            VideoSubscriptionMessage::StartLowQuality => {
                let Some((quality , quality_layer)) = self.quality_layers.iter().next() else { return; };
                self.waiting_high_quality = false;
                self.packet_forwarder.send(VideoPacketForwarderMessage::Start {
                    quality: *quality,
                    gateway_router: quality_layer.gateway_router.clone()
                })
                .await
                .ok_or_terminate(ctx);
            }
            VideoSubscriptionMessage::AddSubsription { quality, layer } => {
                self.quality_layers.insert(quality, layer.clone());
                if self.waiting_high_quality && quality == StreamQuality::High {
                    self.waiting_high_quality = false;
                    self.active_quality = quality;
                    self.packet_forwarder
                        .send(VideoPacketForwarderMessage::Start {
                            quality,
                            gateway_router: layer.gateway_router
                        })
                        .await
                        .ok_or_terminate(ctx);
                }
            }
            VideoSubscriptionMessage::ForcePli => 
            if let Some(layer) = self.quality_layers.get(&self.active_quality) {
                layer.keyframe_interceptor.send(RequestKeyframe).await.ok_or_terminate(ctx);
            }
            VideoSubscriptionMessage::SwitchQualityLayer { to  } => {
                if let Some(layer) = self.quality_layers.get(&to) {
                    self.active_quality = to;
                    self.packet_forwarder
                        .send(VideoPacketForwarderMessage::LayerSwitched {
                            gateway_router: layer.gateway_router.clone()
                        })
                        .await
                        .ok_or_terminate(ctx);
                }
            },
            VideoSubscriptionMessage::Drop => self.stop(ctx).await,
        }
    }

    async fn starting(&mut self, ctx: &crate::actor::Ctx<'_, Self>) {
        let active_track = self.active_track.clone();
        let addr = ctx.addr.clone();
        let forwarder = self.packet_forwarder.clone();
        tokio::spawn(async move {
            let mut rtcp_buf = vec![0u8; 1500];
            while let Ok((packets, _)) = active_track.read(&mut rtcp_buf).await {
                for packet in packets {
                    if packet.as_any().downcast_ref::<PictureLossIndication>().is_some() {
                        if addr.send(VideoSubscriptionMessage::ForcePli).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    if let Some(nack) = packet.as_any().downcast_ref::<TransportLayerNack>() {
                        let mut missing_seqs = Vec::new();
                        for pair in &nack.nacks {
                            missing_seqs.push(pair.packet_id);
                            let mut packet_list = pair.packet_list();
                            missing_seqs.append(&mut packet_list);
                        }
                        if forwarder.send(VideoPacketForwarderMessage::MissedPackets(missing_seqs)).await.is_err() {
                            return;
                        };
                    }
                }
            }
        });
        match self.active_quality {
            StreamQuality::High => if let Some(layer) = self.quality_layers.get(&self.active_quality) {
                 let _ = self.packet_forwarder
                        .send(VideoPacketForwarderMessage::Start {
                            quality: StreamQuality::High,
                            gateway_router: layer.gateway_router.clone()
                        })
                        .await;
            },
            _ => {
                let addr = ctx.addr.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_micros(300)).await;
                    let _ = addr.send(VideoSubscriptionMessage::StartLowQuality).await;
                });
            }
        }
    }

    async fn stopping(mut self, _: &crate::actor::Ctx<'_, Self>) {
        self.packet_forwarder.terminate().await.ok();
        self.quality_layers.clear();
        if let Err(e) = self.pc.remove_track(&self.active_track).await {
            tracing::error!("[VideoSubscription] remove_track {e}");
        }
    }
}