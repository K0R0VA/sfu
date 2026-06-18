use std::sync::Arc;

use crate::{
    PacketAudioSubscription, SyncChannel,
    actor::{Actor, Addr},
    audio_packet_forwarder::AudioPacketForwarder,
    error::Error,
    rtp_packet_forwarder::RtpPacketGatewayRouterMessage,
    user::{ConnectionRequest, User, UserMessage},
};
use uuid::Uuid;
use webrtc::{
    peer_connection::RTCPeerConnection,
    rtp_transceiver::{
        RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection,
    },
    track::track_local::track_local_static_rtp::TrackLocalStaticRTP,
};

pub struct AudioSubscription<S: SyncChannel> {
    pc: Arc<RTCPeerConnection>,
    peer_id: Uuid,
    user: Addr<User<S>>,
    sender: Arc<RTCRtpSender>,
    connection: PacketAudioSubscription,
    forwarder: Addr<AudioPacketForwarder>,
}

impl<S: SyncChannel> AudioSubscription<S> {
    pub async fn init(pc: Arc<RTCPeerConnection>, user: Addr<User<S>>, request: ConnectionRequest<PacketAudioSubscription>) -> Result<Self, Error> {
        let ConnectionRequest {
            codec_mime_type,
            peer_id,
            stream: connection,
            ..
        } = request;
        let output_track: Arc<TrackLocalStaticRTP> = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: codec_mime_type.to_string(),
                ..Default::default()
            },
            format!("audio_{peer_id}"),
            format!("audio_{peer_id}")
        ));
        let sender = pc
            .add_transceiver_from_track(
                output_track.clone() as Arc<_>,
                Some(RTCRtpTransceiverInit {
                    direction: RTCRtpTransceiverDirection::Sendonly,
                    send_encodings: vec![],
                }),
            )
            .await?
            .sender()
            .await;
        let forwarder = AudioPacketForwarder { track: output_track.clone() }
            .start_with_capacity(256);
        let _ = connection
            .rtp_packet_forwarder
            .send(RtpPacketGatewayRouterMessage::Subscribe(forwarder.clone()))
            .await;
        Ok(Self {
            sender,
            pc,
            peer_id,
            user,
            connection,
            forwarder,
        })
    }
}

pub enum Close {
    UserDisconnect,
    StreamForwardFail,
}

impl<S: SyncChannel> Actor for AudioSubscription<S> {
    type Message = Close;
    async fn starting(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[AudioSubscription] starting");
    }
    async fn handle(&mut self, _ctx: &mut crate::actor::Ctx<'_, Self>, m: Self::Message) {
        if let Close::StreamForwardFail = m {
            let _ = self.user.send(UserMessage::Unsubscribe { user_id: self.peer_id }).await;
        }
    }
    async fn stopping(self, _ctx: &crate::actor::Ctx<'_, Self>) {
        let _ = self
            .connection
            .rtp_packet_forwarder
            .send(RtpPacketGatewayRouterMessage::Unsubscribe(self.forwarder.clone()))
            .await;
        self.forwarder.terminate().await;
        if let Err(e) = self.pc.remove_track(&self.sender).await {
            tracing::error!("[AudioSubscription] remove_track failed {e}");
        }
    }
}
