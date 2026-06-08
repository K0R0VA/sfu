use std::sync::Arc;

use tokio::task::AbortHandle;
use uuid::Uuid;
use webrtc::{peer_connection::RTCPeerConnection, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::track_local_static_rtp::TrackLocalStaticRTP};

use crate::{PacketSubscription, actor::{Actor, Addr}, error::Error, packet_subscription::PacketForwarder, user::{ConnectionRequest, User, UserMessage}};

pub struct AudioSubscription {
    pc: Arc<RTCPeerConnection>,
    speaker_id: Uuid,
    user: Addr<User>,
    sender: Arc<RTCRtpSender>,
    output_track: Arc<TrackLocalStaticRTP>,
    stream: PacketSubscription,
    forwarder: Option<Addr<PacketForwarder>>,
    task: Option<AbortHandle>
}

impl AudioSubscription {
    pub async fn init(
        pc: Arc<RTCPeerConnection>, 
        user: Addr<User>,
        request: ConnectionRequest
    ) -> Result<Self, Error> {
        let ConnectionRequest { codec_mime_type, speaker_id, stream, .. } = request;
        let output_track: Arc<TrackLocalStaticRTP> = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: codec_mime_type,
                ..Default::default()
            },
            Uuid::new_v4().to_string(),
            speaker_id.to_string(),
        ));
        let sender = pc.add_transceiver_from_track(
                output_track.clone() as Arc<_>,
                Some(RTCRtpTransceiverInit { direction: RTCRtpTransceiverDirection::Sendonly, send_encodings: vec![] })
            )
            .await?
            .sender()
            .await;
        Ok(Self {
            sender,
            pc,
            speaker_id,
            output_track,
            user,
            stream,
            task: None,
            forwarder: None
        })
    }
}

pub enum Close {
    UserDisconnect,
    StreamForwardFail
}

impl Actor for AudioSubscription {
    type Message = Close;
    async fn starting(&mut self, ctx: &crate::actor::Ctx<'_, Self>) {
        let forwarder = PacketForwarder { 
            track: self.output_track.clone(), 
            owner: futures_util::future::Either::Left(ctx.addr.clone()) 
        }.start();
        let stream = tokio_stream::wrappers::BroadcastStream::new(self.stream.stream.resubscribe());
        let task = forwarder.add_stream(stream, |r| match r {
            Ok(packet) => crate::actor::StreamItem::Next(packet),
            Err(_) => crate::actor::StreamItem::Close
        });
        self.task = Some(task);
        self.forwarder = Some(forwarder);
        self.stream.active_receiver_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::info!("[AudioSubscription] starting");
    }
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, m: Self::Message) {
        if let Close::StreamForwardFail = m {
            let _ = self.user.send(UserMessage::DisconnectFromUser { speaker_id: self.speaker_id }).await;
        }
        self.stop(ctx).await;
    }
    async fn stopping(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        if let Some(task) = self.task.take() {
            task.abort()
        }
        if let Err(e) = self.pc.remove_track(&self.sender).await {
            tracing::error!("[AudioSubscription] remove_track failed {e}");
        }
    }
}