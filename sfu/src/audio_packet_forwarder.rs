use std::sync::Arc;

use rtc::{media_stream::MediaStreamTrack, rtp::Packet, rtp_transceiver::{RTCRtpTransceiverDirection, RTCRtpTransceiverInit, rtp_sender::{RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind}}};
use webrtc::{media_stream::track_local::{TrackLocal, static_rtp::TrackLocalStaticRTP}, peer_connection::{PeerConnection, RTCPeerConnection}};

use crate::{actor::{Actor, Addr}, error::Error, rtp_packet_gateway_router::{AudioRouterContext, RtpPacketGatewayRouter, RtpPacketGatewayRouterMessage, RtpPacketMessage}, server::Key, user::ConnectionRequest};

pub struct AudioPacketForwarder {
    pub track: Arc<dyn TrackLocal>,
    pub ssrc: u32,
    pub router: Addr<RtpPacketGatewayRouter<Self, AudioRouterContext>>
}

impl AudioPacketForwarder {
    pub async fn init<K: Key>(pc: &Box<dyn PeerConnection>, request: ConnectionRequest<K, AudioPacketForwarder, AudioRouterContext>) -> Result<Self, Error> {
        let ConnectionRequest {
            peer_id,
            codec_mime_type,
            gateway_router,
            ..
        } = request;
        let stream_id = format!("audio_{peer_id}");
        let ssrc = rand::random();
        let track = MediaStreamTrack::new(
                stream_id.clone(),  
                stream_id.clone(),
            "Microphone".to_string(),
            RtpCodecKind::Audio,
            vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(ssrc),
                    ..Default::default()
                },
                codec: RTCRtpCodec {
                    mime_type: codec_mime_type.to_string(),
                    ..Default::default()
                },
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
                    mime_type: codec_mime_type.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            }] 
        })).await?;
        let forwarder = Self { track: output_track, router: gateway_router, ssrc };
        Ok(forwarder)
    }
    async fn forward(&self, mut r: Packet) -> Result<(), Error> {
        r.header.ssrc = self.ssrc;
        self.track.write_rtp(r).await?;
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

