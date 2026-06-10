use std::{collections::HashMap, sync::{Arc, atomic::Ordering}, time::Duration};

use tokio::task::AbortHandle;
use uuid::Uuid;
use webrtc::{peer_connection::RTCPeerConnection, rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::{track_local_static_rtp::TrackLocalStaticRTP, track_local_static_sample::TrackLocalStaticSample}};

use crate::{PacketVideoSubscription, actor::{Actor, Addr}, error::Error, pli_sender::Ping, quality_monitor::{QualityMonitor, QualityMonitorMessage}, room::StreamQuality, video_packet_forwarder::{VideoPacketForwarder, VideoPacketForwarderMessage}, video_subscription::VideoSubscriptionMessage::ForcePli};

pub struct VideoSubscription {
    pub pc: Arc<RTCPeerConnection>,
    pub peer_id: Uuid,
    pub connections: HashMap<StreamQuality, PacketVideoSubscription>,
    pub active_track: Arc<RTCRtpSender>,
    pub task: Option<AbortHandle>,
    pub packet_forwarder: Addr<VideoPacketForwarder>,
    pub active_quality: StreamQuality,
    pub is_active: bool,
}

impl VideoSubscription {
    pub async fn new(pc: Arc<RTCPeerConnection>, peer_id: Uuid, mime_type: String, stream: PacketVideoSubscription, quality: StreamQuality) -> Result<Self, Error> {
        let mut connections = HashMap::with_capacity(3);
        let track: Arc<TrackLocalStaticRTP> = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: mime_type.clone(),
                ..Default::default()
            },
            "video".to_string(),
            peer_id.to_string(),
        ));
        let active_track = pc.add_transceiver_from_track(
                track.clone() as Arc<_>,
                Some(RTCRtpTransceiverInit { direction: RTCRtpTransceiverDirection::Sendonly, send_encodings: vec![] })
            )
            .await?
            .sender()
            .await;
        let packet_forwarder = VideoPacketForwarder::new(track.clone()).start();
        connections.insert(quality, stream.clone());
        let this = Self { pc, peer_id, connections, active_track, active_quality: quality, packet_forwarder, task: None, is_active: false  };
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
                tracing::info!("[VideoSubscription] SwitchQualityLayer {:?}", to);
                if let Some(subscription) = self.connections.get(&to) {
                    if let Some(connection) = self.connections.get(&self.active_quality) {
                        connection.active_receiver_counter.fetch_sub(1, Ordering::Relaxed);
                    }
                    subscription.active_receiver_counter.fetch_add(1, Ordering::Relaxed);
                    let _ = self.packet_forwarder.send(VideoPacketForwarderMessage::LayerSwitched).await;
                    let _ = subscription.pli_sender.send(Ping).await;
                    let stream = tokio_stream::wrappers::BroadcastStream::new(subscription.stream.resubscribe());
                    let task = self.packet_forwarder.add_stream(stream, |r| match r {
                        Ok(packet) => crate::actor::StreamItem::Next(VideoPacketForwarderMessage::RtpPacket(packet)),
                        Err(_) => crate::actor::StreamItem::Close
                    });
                    if let Some(old_task) = self.task.take() {
                        old_task.abort();
                    }
                    self.task = Some(task);
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
                    // Если подписчик кричит, что потерял картинку (PLI)
                    if packet.as_any().downcast_ref::<PictureLossIndication>().is_some() {
                        let _ = addr.send(ForcePli).await;
                    }
                }
            }
        });
        let subscription = self.connections.values().next().unwrap();
        let _ = self.packet_forwarder.send(VideoPacketForwarderMessage::LayerSwitched).await;
        subscription.active_receiver_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let stream = tokio_stream::wrappers::BroadcastStream::new(subscription.stream.resubscribe());
        let task = self.packet_forwarder.add_stream(stream, |r| match r {
            Ok(packet) => crate::actor::StreamItem::Next(VideoPacketForwarderMessage::RtpPacket(packet)),
            Err(e) => {
                tracing::error!("{e}");
                crate::actor::StreamItem::Close
            }
        });
        self.task = Some(task);
        tracing::info!("[VideoSubscription] starting");
    }

    async fn stopping(&mut self, _: &crate::actor::Ctx<'_, Self>) {
        if let Some(active_connection) = self.connections.get(&self.active_quality) {
            active_connection.active_receiver_counter.fetch_sub(1, Ordering::Relaxed);
        }
        self.connections.clear();
        if let Some(task) = self.task.take() {
            task.abort();
        };
        if let Err(e) = self.pc.remove_track(&self.active_track).await {
            tracing::error!("[VideoSubscription] remove_track {e}");
        }
    }
}