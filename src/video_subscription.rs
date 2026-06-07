use std::{collections::HashMap, sync::{Arc, atomic::Ordering}};

use uuid::Uuid;
use webrtc::{peer_connection::RTCPeerConnection, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::track_local_static_rtp::TrackLocalStaticRTP};

use crate::{PacketSubscription, actor::{Actor, Addr}, error::Error, quality_monitor::QualityMonitor, room::StreamQuality, user::handle_rtp_packets};

pub struct VideoSubscription {
    pub pc: Arc<RTCPeerConnection>,
    pub peer_id: Uuid,
    pub connections: HashMap<StreamQuality, PacketSubscription>,
    pub mime_type: String,
    pub active_track: Arc<RTCRtpSender>,
    pub track: Arc<TrackLocalStaticRTP>,
    pub drop_call: Option<tokio::sync::oneshot::Sender<()>>,
    pub quality_monitor: Option<Addr<QualityMonitor>>
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
        let mut this = Self { pc, peer_id, connections, active_track, track, mime_type, drop_call: None, quality_monitor: None };
        this.activate_stalled_connection(stream).await?;
        Ok(this) 
    }
}

impl VideoSubscription {
    async fn activate_stalled_connection(&mut self, stream: PacketSubscription) -> Result<(), Error> {
        let output_track = self.track.clone();
        let (drop, is_dropped) = tokio::sync::oneshot::channel();
        if let Some(drop) = self.drop_call.take() {
            let _ = drop.send(());
        }
        self.drop_call = Some(drop);
        tokio::spawn(async move {
            let PacketSubscription { mut stream, active_receiver_counter } = stream;
            active_receiver_counter.fetch_add(1, Ordering::Relaxed);
            tokio::select! {
                result = handle_rtp_packets(&mut stream, output_track) => {
                    if let Err(e) = result {
                        tracing::error!("[VideoSubscription] {e}");
                    }
                },
                _ = is_dropped => {}
            }
            active_receiver_counter.fetch_sub(1, Ordering::Relaxed);
        });
        Ok(())
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
                let Some(stream) = self.connections.get(&to) else { return; };
                if let Err(e) = self.activate_stalled_connection(stream.clone()).await {
                    tracing::error!("[VideoSubscription] SwitchQualityLayer {e}");
                    self.stop(ctx).await;
                }
            },
            VideoSubscriptionMessage::Drop => self.stop(ctx).await
        }
    }

    async fn starting(&mut self, ctx: &crate::actor::Ctx<'_, Self>) {
        let pc = self.pc.clone();
        self.quality_monitor = Some(QualityMonitor::new(pc, ctx.addr.clone()).start());
        tracing::info!("[VideoSubscription] starting");
    }

    async fn stopping(&mut self, _: &crate::actor::Ctx<'_, Self>) {
        self.connections.clear();
        if let Some(drop) = self.drop_call.take() {
            let _ = drop.send(());
        };
        if let Err(e) = self.pc.remove_track(&self.active_track).await {
            tracing::error!("[VideoSubscription] remove_track {e}");
        }
    }
}