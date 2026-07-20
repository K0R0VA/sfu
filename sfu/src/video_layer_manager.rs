use std::{collections::{HashMap}, sync::Arc, time::Duration};
use tokio::sync::Notify;
use uuid::Uuid;
use webrtc::{peer_connection::RTCPeerConnection, rtcp::{payload_feedbacks::picture_loss_indication::PictureLossIndication, transport_feedbacks::transport_layer_nack::TransportLayerNack}, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::track_local_static_rtp::TrackLocalStaticRTP};

use crate::{actor::{Actor, Addr, StoppingExt, WeakAddr}, error::Error, keyframe_interceptor::{KeyframeInterceptor, RequestKeyframe}, room::{Codek, StreamQuality}, rtp_packet_gateway_router::{RtpPacketGatewayRouter, VideoRouterContext}, video_packet_forwarder::{VideoPacketForwarder, VideoPacketForwarderMessage}};

pub struct VideoLayerManager {
    pub pc: Arc<RTCPeerConnection>,
    pub peer_id: Uuid,
    pub quality_layers: HashMap<StreamQuality, QualityLayer>,
    pub active_track: Arc<RTCRtpSender>,
    pub track: Arc<TrackLocalStaticRTP>,
    pub packet_forwarder: WeakAddr<VideoPacketForwarder>,
    pub connection_quality: StreamQuality,
    pub active_quality: Option<StreamQuality>,
    pub is_forwarder_running: bool,
}

#[derive(Clone)]
pub struct QualityLayer {
    pub gateway_router: Addr<RtpPacketGatewayRouter<VideoPacketForwarder, VideoRouterContext>>,
    pub keyframe_interceptor: Addr<KeyframeInterceptor>,
    pub wake_notification: Arc<Notify>
}

impl VideoLayerManager {
    pub async fn new(
        pc: Arc<RTCPeerConnection>, 
        peer_id: Uuid, 
        codek: Codek, 
        current_connection_quality: StreamQuality,
        quality: StreamQuality,
        quality_layer: QualityLayer,
    ) -> Result<Self, Error> {
        let mut quality_layers = HashMap::new();
        let track: Arc<TrackLocalStaticRTP> = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: codek.to_string(),
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
        quality_layers.insert(quality, quality_layer);
        let this = Self { pc, peer_id, track, quality_layers, active_track, active_quality: None, connection_quality: current_connection_quality, packet_forwarder: WeakAddr::default(), is_forwarder_running: false };
        Ok(this) 
    }
    fn spawn_notify_task(&self, addr: Addr<Self>, quality: StreamQuality, wake_notification: Arc<Notify> ) {
        tokio::spawn(async move {
            loop {
                wake_notification.notified().await;
                let message = VideoLayerManagerMessage::LayerAwake { quality };
                let send_fut = addr.send(message).await;
                if send_fut.is_err() {
                    break;
                }
            }
        });
    }
}

pub enum VideoLayerManagerMessage {
    AddLayer { quality: StreamQuality, layer: QualityLayer },
    RunFirstLayer,
    SwitchQualityLayer { to: StreamQuality },
    SwitchToLowQuality,
    LayerAwake { quality: StreamQuality },
    Drop,
    ForcePli
}


impl Actor for VideoLayerManager {
    type Message = VideoLayerManagerMessage;
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, m: Self::Message) {
        match m {
            VideoLayerManagerMessage::AddLayer { quality, layer } => {
                self.quality_layers.insert(quality, layer.clone());
                self.spawn_notify_task(ctx.addr.clone(), quality, layer.wake_notification);
                if quality == self.connection_quality {
                    let message = match self.is_forwarder_running {
                        true => VideoPacketForwarderMessage::LayerSwitched {
                            gateway_router: layer.gateway_router.clone()
                        },
                        false => VideoPacketForwarderMessage::Start {
                            quality,
                            gateway_router: layer.gateway_router
                        }
                    };
                    self.active_quality = Some(quality);
                    self.is_forwarder_running = true;
                    self.packet_forwarder
                        .try_send(message)
                        .await
                        .ok_or_terminate(ctx);
                }
            }
            VideoLayerManagerMessage::ForcePli => {
                let Some(active_quality) = self.active_quality else { return ; };
                let Some(layer) = self.quality_layers.get(&active_quality) else { return ; }; 
                layer.keyframe_interceptor.send(RequestKeyframe).await.ok_or_terminate(ctx);
            }
            VideoLayerManagerMessage::SwitchQualityLayer { to  } => {
                let Some(layer) = self.quality_layers.get(&to) else { tracing::warn!("Missing layer"); return ; };
                self.active_quality = Some(to);
                self.connection_quality = to;
                self.packet_forwarder
                    .try_send(VideoPacketForwarderMessage::LayerSwitched {
                        gateway_router: layer.gateway_router.clone()
                    })
                    .await
                    .ok_or_terminate(ctx);
            },
            VideoLayerManagerMessage::SwitchToLowQuality => {
                if let Some(layer) = self.quality_layers.get(&StreamQuality::Low) {
                self.active_quality = Some(StreamQuality::Low);
                self.packet_forwarder
                    .try_send(VideoPacketForwarderMessage::LayerSwitched {
                        gateway_router: layer.gateway_router.clone()
                    })
                    .await
                    .ok_or_terminate(ctx);
                }
            },
            VideoLayerManagerMessage::LayerAwake { quality } => 
            if self.connection_quality == quality && self.active_quality != Some(quality) {
                let Some(layer) = self.quality_layers.get(&quality) else { return ; };
                self.active_quality = Some(quality);
                self.packet_forwarder
                    .try_send(VideoPacketForwarderMessage::LayerSwitched {
                        gateway_router: layer.gateway_router.clone()
                    })
                    .await
                    .ok_or_terminate(ctx);
            }
            VideoLayerManagerMessage::RunFirstLayer => if !self.is_forwarder_running {
                self.is_forwarder_running = true;
                if let Some((quality, layer)) = self.quality_layers.iter().next() {
                    self.active_quality = Some(*quality);
                    let message = VideoPacketForwarderMessage::Start {
                        quality: *quality,
                        gateway_router: layer.gateway_router.clone()
                    };
                    self.packet_forwarder
                        .try_send(message)
                        .await
                        .ok_or_terminate(ctx);
                }
            }
            VideoLayerManagerMessage::Drop => self.stop(ctx).await,
        }
    }

    async fn starting(&mut self, ctx: &crate::actor::Ctx<'_, Self>) {
        let active_track = self.active_track.clone();
        let addr = ctx.addr.clone();
        let packet_forwarder = VideoPacketForwarder::new(self.track.clone(), addr.clone())
            .start_with_capacity(2048);
        let forwarder = packet_forwarder.clone();
        self.packet_forwarder.set_addr(packet_forwarder);
        if let Some((quality, layer)) = self.quality_layers.iter().next() {
            let wake_notification = layer.wake_notification.clone();
            self.spawn_notify_task(addr.clone(), *quality, wake_notification);
        }
        tokio::spawn(async move {
            loop {
                let mut rtcp_buf = [0u8; 1500];
                let Ok((packets, _)) = active_track.read(&mut rtcp_buf).await else { break; };
                for packet in packets {
                    if packet.as_any().downcast_ref::<PictureLossIndication>().is_some() {
                        let result = addr.send(VideoLayerManagerMessage::ForcePli).await;
                        if result.is_err() {
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
                        let message= VideoPacketForwarderMessage::MissedPackets(missing_seqs);
                        let result = forwarder.send(message).await;
                        if result.is_err() {
                            return;
                        };
                    }
                }
            }
        });
        let wait_period = Duration::from_millis(500);
        if let Some(layer) = self.quality_layers.get(&self.connection_quality) {
            let _ = self.packet_forwarder
                        .try_send(VideoPacketForwarderMessage::Start {
                            quality: self.connection_quality,
                            gateway_router: layer.gateway_router.clone()
                        })
                        .await;
            self.is_forwarder_running = true;
        } else {
            let addr = ctx.addr.clone();
            tokio::spawn(async move {
                tokio::time::sleep(wait_period).await;
                let _ = addr.send(VideoLayerManagerMessage::RunFirstLayer).await;
            });
        }
    }

    async fn stopping(mut self, _: &crate::actor::Ctx<'_, Self>) {
        self.packet_forwarder.try_terminate().await.ok();
        self.quality_layers.clear();
        if let Err(e) = self.pc.remove_track(&self.active_track).await {
            tracing::error!("[VideoSubscription] remove_track {e}");
        }
    }
}