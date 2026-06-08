use std::{collections::HashMap, sync::{Arc, atomic::Ordering}};

use tokio::task::AbortHandle;
use uuid::Uuid;
use webrtc::{peer_connection::RTCPeerConnection, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::track_local_static_rtp::TrackLocalStaticRTP};

use crate::{PacketSubscription, actor::{Actor, Addr}, error::Error, packet_subscription::PacketForwarder, quality_monitor::{QualityMonitor, QualityMonitorMessage}, room::StreamQuality, user::handle_rtp_packets};

pub struct VideoSubscription {
    pub pc: Arc<RTCPeerConnection>,
    pub peer_id: Uuid,
    pub connections: HashMap<StreamQuality, PacketSubscription>,
    pub active_track: Arc<RTCRtpSender>,
    pub track: Arc<TrackLocalStaticRTP>,
    pub task: Option<AbortHandle>,
    pub quality_monitor: Option<Addr<QualityMonitor>>,
    pub packet_forwarder: Option<Addr<PacketForwarder>>,
    pub active_quality: StreamQuality
}

impl VideoSubscription {
    pub async fn new(pc: Arc<RTCPeerConnection>, peer_id: Uuid, mime_type: String, stream: PacketSubscription, quality: StreamQuality) -> Result<Self, Error> {
        let mut connections = HashMap::with_capacity(3);
        let track: Arc<TrackLocalStaticRTP> = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: mime_type.clone(),
                ..Default::default()
            },
            Uuid::new_v4().to_string(),
            peer_id.to_string(),
        ));
        let active_track = pc.add_transceiver_from_track(
                track.clone() as Arc<_>,
                Some(RTCRtpTransceiverInit { direction: RTCRtpTransceiverDirection::Sendonly, send_encodings: vec![] })
            )
            .await?
            .sender()
            .await;
        connections.insert(quality, stream.clone());
        let this = Self { pc, peer_id, connections, active_track, track, active_quality: quality, quality_monitor: None, packet_forwarder: None, task: None };
        Ok(this) 
    }
}

pub enum VideoSubscriptionMessage {
    AddSubsription { quality: StreamQuality, stream: PacketSubscription },
    SwitchQualityLayer { to: StreamQuality },
    Drop
}


impl Actor for VideoSubscription {
    type Message = VideoSubscriptionMessage;

    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, m: Self::Message) {
        match m {
            VideoSubscriptionMessage::AddSubsription { quality, stream } => {
                self.connections.insert(quality, stream);
            }
            VideoSubscriptionMessage::SwitchQualityLayer { to  } => {
                let Some(subscription) = self.connections.get(&to) else { return; };
                let Some(packet_forwarder) = &self.packet_forwarder else { return ; };
                let stream = tokio_stream::wrappers::BroadcastStream::new(subscription.stream.resubscribe());
                if let Some(pli_channel) = &subscription.pli_channel {
                    let _ = pli_channel.send(()).await;
                }
                let task = packet_forwarder.add_stream(stream, |r| match r {
                    Ok(packet) => crate::actor::StreamItem::Next(packet),
                    Err(_) => crate::actor::StreamItem::Close
                });
                if let Some(connection) = self.connections.get(&self.active_quality) {
                    connection.active_receiver_counter.fetch_sub(1, Ordering::Relaxed);
                }
                if let Some(old_task) = self.task.take() {
                    old_task.abort();
                }
                self.task = Some(task);
            },
            VideoSubscriptionMessage::Drop => self.stop(ctx).await
        }
    }

    async fn starting(&mut self, ctx: &crate::actor::Ctx<'_, Self>) {
        let pc = self.pc.clone();
        self.quality_monitor = Some(QualityMonitor::new(pc, ctx.addr.clone(), self.active_quality).start());
        if let Some(subscription) = self.connections.values().next() {
            let packet_forwarder = PacketForwarder { owner: futures_util::future::Either::Right(ctx.addr.clone()), track: self.track.clone() }.start();
            let stream = tokio_stream::wrappers::BroadcastStream::new(subscription.stream.resubscribe());
            if let Some(pli_channel) = &subscription.pli_channel {
                let _ = pli_channel.send(()).await;
            }
            let task = packet_forwarder.add_stream(stream, |r| match r {
                Ok(packet) => crate::actor::StreamItem::Next(packet),
                Err(_) => crate::actor::StreamItem::Close
            });
            self.task = Some(task);
            self.packet_forwarder = Some(packet_forwarder);
            subscription.active_receiver_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        tracing::info!("[VideoSubscription] starting");
    }

    async fn stopping(&mut self, _: &crate::actor::Ctx<'_, Self>) {
        if let Some(active_connection) = self.connections.get(&self.active_quality) {
            active_connection.active_receiver_counter.fetch_sub(1, Ordering::Relaxed);
        }
        self.connections.clear();
        if let Some(quality_monitor) = self.quality_monitor.take() {
            let _ = quality_monitor.send(QualityMonitorMessage::Close).await;
        }
        if let Some(task) = self.task.take() {
            task.abort();
        };
        if let Err(e) = self.pc.remove_track(&self.active_track).await {
            tracing::error!("[VideoSubscription] remove_track {e}");
        }
    }
}