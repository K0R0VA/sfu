use std::sync::Arc;

use crate::{
    SyncChannel, actor::{Actor, Addr, StoppingExt}, audio_packet_forwarder::AudioPacketForwarder, error::Error, rtp_packet_gateway_router::{ RtpPacketGatewayRouter, RtpPacketGatewayRouterMessage}, user::{ConnectionRequest, User, UserMessage},
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
    user_addr: Addr<User<S>>,
    sender: Arc<RTCRtpSender>,
    forwarder: Addr<AudioPacketForwarder>,
    gateway_router: Addr<RtpPacketGatewayRouter<AudioPacketForwarder>>,
}

impl<S: SyncChannel> AudioSubscription<S> {
    pub async fn init(pc: Arc<RTCPeerConnection>, user: Addr<User<S>>, request: ConnectionRequest<AudioPacketForwarder>) -> Result<Self, Error> {
        let ConnectionRequest {
            codec_mime_type,
            peer_id,
            gateway_router,
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
        let tranceiver = pc
            .add_transceiver_from_track(
                output_track.clone() as Arc<_>,
                Some(RTCRtpTransceiverInit {
                    direction: RTCRtpTransceiverDirection::Sendonly,
                    send_encodings: vec![],
                }),
            )
            .await?;
        let sender = tranceiver.sender().await;
        let forwarder = AudioPacketForwarder { track: output_track.clone() }
            .start_with_capacity(256);
        gateway_router
            .send(RtpPacketGatewayRouterMessage::Subscribe(forwarder.clone()))
            .await?;
        Ok(Self {
            sender,
            pc,
            peer_id,
            user_addr: user,
            forwarder,
            gateway_router
        })
    }
    async fn try_stop(self) -> Result<(), Error> {
        self
            .gateway_router
            .send(RtpPacketGatewayRouterMessage::Unsubscribe(self.forwarder.clone()))
            .await?;
        self.forwarder.terminate().await?;
        self.pc.remove_track(&self.sender).await?;
        Ok(())
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
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, m: Self::Message) {
        if let Close::StreamForwardFail = m {
            self.user_addr.send(UserMessage::Unsubscribe { user_id: self.peer_id }).await.ok_or_terminate(ctx);
        }
    }
    async fn stopping(self, _ctx: &crate::actor::Ctx<'_, Self>) {
        if let Err(e) = self.try_stop().await {
            tracing::error!("[AudioSubscription] remove_track failed {e}");
        }
    }
}
