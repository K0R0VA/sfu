use std::sync::Arc;

use webrtc::{peer_connection::RTCPeerConnection, rtp::packet::Packet, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::{TrackLocalWriter, track_local_static_rtp::TrackLocalStaticRTP}};

use crate::{actor::{Actor, Addr, StoppingExt}, error::Error, rtp_packet_gateway_router::{AudioRouterContext, RtpPacketGatewayRouter, RtpPacketGatewayRouterMessage, RtpPacketMessage}, user::ConnectionRequest};

pub struct AudioPacketForwarder {
    pub track: Arc<TrackLocalStaticRTP>,
    pub sender: Arc<RTCRtpSender>,
    pub router: Addr<RtpPacketGatewayRouter<Self, AudioRouterContext>>
}

impl AudioPacketForwarder {
    pub async fn init(pc: Arc<RTCPeerConnection>, request: ConnectionRequest<AudioPacketForwarder, AudioRouterContext>) -> Result<Self, Error> {
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
        let forwarder = Self { track: output_track, sender, router: gateway_router };
        Ok(forwarder)
    }
    async fn forward(&self, r: Packet) -> Result<(), Error> {
        self.track.write_rtp(&r).await?;
        Ok(())
    }
}

pub struct AudioPacketForwarderMessage {
    packet: Option<Packet>
}

impl From<RtpPacketMessage> for AudioPacketForwarderMessage {
    fn from(message: RtpPacketMessage) -> Self {
        let packet = match message {
            RtpPacketMessage::Packet(_, packet) => Some(packet),
            _ => None
        };
        Self {packet}
    }
}

impl Actor for AudioPacketForwarder {
    type Message = AudioPacketForwarderMessage;
    async fn starting(&mut self, ctx: &crate::actor::Ctx<'_, Self>) {
        let _ = self.router
            .send(RtpPacketGatewayRouterMessage::Subscribe(ctx.addr.clone()))
            .await;
    }
    async fn handle(&mut self, _ctx: &mut crate::actor::Ctx<'_, Self>, packet: Self::Message) {
        if let Some(packet) = packet.packet {
            let _ = self.forward(packet).await;
        }
    }
    async fn stopping(self, ctx: &crate::actor::Ctx<'_, Self>) {
        let _ = self.router
            .send(RtpPacketGatewayRouterMessage::Unsubscribe(ctx.addr.clone()))
            .await;
    }
}

