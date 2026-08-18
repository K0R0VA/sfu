use std::{collections::{HashMap}, sync::Arc};
use rtc::{media_stream::MediaStreamTrack, rtcp::{payload_feedbacks::picture_loss_indication::PictureLossIndication, transport_feedbacks::transport_layer_nack::TransportLayerNack}, rtp_transceiver::{RTCRtpTransceiverDirection, RTCRtpTransceiverInit, rtp_sender::{RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind}}};
use uuid::Uuid;
use webrtc::{media_stream::{track_local::{TrackLocal, TrackLocalEvent, static_rtp::TrackLocalStaticRTP}}, peer_connection::PeerConnection};

use crate::{actor::{Actor, Addr, StoppingExt}, error::Error, keyframe_interceptor::{KeyframeInterceptor, RequestKeyframe}, room::{Codec, StreamQuality}, rtp_packet_gateway_router::{RouterWaker, RtpPacketGatewayRouter, VideoRouterContext}, server::Key, video_packet_forwarder::{VideoPacketForwarder, VideoPacketForwarderMessage}};

pub struct VideoLayerManager {
    pub quality_layers: HashMap<StreamQuality, QualityLayer>,
    pub packet_forwarder: Addr<VideoPacketForwarder>,
    pub connection_quality: StreamQuality,
    pub active_quality: Option<StreamQuality>,
    pub track: Arc<dyn TrackLocal>,
    pub timeout_channel: Option<tokio::sync::mpsc::Receiver<()>>
}

#[derive(Clone)]
pub struct QualityLayer {
    pub gateway_router: Addr<RtpPacketGatewayRouter<VideoPacketForwarder, VideoRouterContext>>,
    pub keyframe_interceptor: Addr<KeyframeInterceptor>,
    pub router_waker: RouterWaker
}

impl VideoLayerManager {
    pub async fn new<K: Key>(
        pc: &Box<dyn PeerConnection>, 
        peer_id: K, 
        codec: Codec, 
        current_connection_quality: StreamQuality,
        quality: StreamQuality,
        quality_layer: QualityLayer,
    ) -> Result<Self, Error> {
        let mut quality_layers = HashMap::new();
        let stream_id = format!("video_{peer_id}");
        let ssrc = rand::random();
        let track = MediaStreamTrack::new(
                stream_id.clone(),  
                Uuid::new_v4().to_string(),
            "Webcam".to_string(),
            RtpCodecKind::Video,
            vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(ssrc),
                    ..Default::default()
                },
                codec: RTCRtpCodec {
                    mime_type: codec.to_string(),
                    ..Default::default()
                },
                active: true,
                ..Default::default()
            }],
        );
        let output_track= Arc::new(TrackLocalStaticRTP::new(track));
        pc.add_transceiver_from_track(output_track.clone(), Some(RTCRtpTransceiverInit { 
            direction: RTCRtpTransceiverDirection::Sendonly, 
            streams: vec![stream_id], 
            send_encodings: vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(ssrc),
                    ..Default::default()
                },
                codec: RTCRtpCodec {
                    mime_type: codec.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            }] 
        })).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let packet_forwarder = VideoPacketForwarder::new(output_track.clone(), tx, ssrc, codec.into())
            .start_with_capacity(2048);
        quality_layers.insert(quality, quality_layer);
        let this = Self { 
            quality_layers, 
            active_quality: None, 
            connection_quality: current_connection_quality, 
            packet_forwarder,
            track: output_track,
            timeout_channel: Some(rx)
        };
        Ok(this) 
    }
    fn spawn_notify_task(&self, addr: Addr<Self>, quality: StreamQuality, router_waker: RouterWaker ) {
        tokio::spawn(async move {
            let mut stream = router_waker.resubscribe();
            while let Ok(_) = stream.recv().await {
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
    ForcePli,
    SwitchQualityLayer { to: StreamQuality },
    FallbackToLowQuality,
    LayerAwake { quality: StreamQuality },
    ResumeStreaming,
    Drop,
}


impl Actor for VideoLayerManager {
    type Message = VideoLayerManagerMessage;
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, m: Self::Message) {
        match m {
            VideoLayerManagerMessage::ForcePli => 
            if let Some(layer) = self.quality_layers.get(&self.active_quality.unwrap_or(self.connection_quality)) {
                layer.keyframe_interceptor.send(RequestKeyframe::Pli).await.ok_or_terminate(ctx);
            }
            VideoLayerManagerMessage::AddLayer { quality, layer } => {
                self.quality_layers.insert(quality, layer.clone());
                self.spawn_notify_task(ctx.addr.clone(), quality, layer.router_waker);
                if quality == self.connection_quality {
                    let message = VideoPacketForwarderMessage::LayerSwitched {
                        quality,
                        gateway_router: layer.gateway_router.clone()
                    };
                    self.active_quality = Some(quality);
                    self.packet_forwarder
                        .send(message)
                        .await
                        .ok_or_terminate(ctx);
                }
            }
            VideoLayerManagerMessage::ResumeStreaming =>  {
                self.packet_forwarder.send(VideoPacketForwarderMessage::Reset).await.ok_or_terminate(ctx);
            }
            VideoLayerManagerMessage::SwitchQualityLayer { to  } => {
                if Some(to) == self.active_quality {
                    self.connection_quality = to;
                    return;
                }
                let Some(layer) = self.quality_layers.get(&to) else { tracing::warn!("Missing layer"); return ; };
                self.active_quality = Some(to);
                self.connection_quality = to;
                self.packet_forwarder
                    .send(VideoPacketForwarderMessage::LayerSwitched {
                        quality: to,
                        gateway_router: layer.gateway_router.clone()
                    })
                    .await
                    .ok_or_terminate(ctx);
            },
            VideoLayerManagerMessage::FallbackToLowQuality  => if self.active_quality != Some(StreamQuality::Low) {
                if let Some(layer) = self.quality_layers.get(&StreamQuality::Low) {
                self.active_quality = Some(StreamQuality::Low);
                self.packet_forwarder
                    .send(VideoPacketForwarderMessage::LayerSwitched {
                        quality: StreamQuality::Low,
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
                    .send(VideoPacketForwarderMessage::LayerSwitched {
                        quality,
                        gateway_router: layer.gateway_router.clone()
                    })
                    .await
                    .ok_or_terminate(ctx);
            }
            VideoLayerManagerMessage::Drop => self.stop(ctx).await,
        }
    }

    async fn starting(&mut self, ctx: &crate::actor::Ctx<'_, Self>) {
        let track = self.track.clone();
        let addr = ctx.addr.clone();
        let forwarder = self.packet_forwarder.clone();
        tokio::spawn(async move {
            while let Some(TrackLocalEvent::OnRtcpPacket(packets)) = track.poll().await {
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
            };
        });
        let addr = ctx.addr.clone();
        if let Some((quality, layer)) = self.quality_layers.iter().next() {
            let wake_notification = layer.router_waker.clone();
            self.spawn_notify_task(addr.clone(), *quality, wake_notification);
            self.active_quality = Some(*quality);
            let _ = self.packet_forwarder
                        .send(VideoPacketForwarderMessage::Start {
                            quality: *quality,
                            gateway_router: layer.gateway_router.clone()
                        })
                        .await;
        } 
        let addr = ctx.addr.clone();
        let mut rx = self.timeout_channel.take().expect("timeout_channel should be provided");
        tokio::spawn(async move {
            while let Some(_) = rx.recv().await {
                if addr.send(VideoLayerManagerMessage::FallbackToLowQuality).await.is_err() {
                    break;
                }
            }
        });
    }
    async fn stopping(self, _: &crate::actor::Ctx<'_, Self>) {
        self.packet_forwarder.terminate().await.ok();
    }
}