use std::{sync::Arc, time::Instant};
use rtc::rtp::Packet;
use webrtc::media_stream::track_local::{TrackLocal};

use crate::{actor::{Actor, Addr, Ctx, StoppingExt}, error::Error, room::StreamQuality, rtp_packet_gateway_router::{RtpPacketGatewayRouter, RtpPacketGatewayRouterMessage, RtpPacketMessage, VideoRouterContext}, };

pub struct VideoPacketForwarder {
    track: Arc<dyn TrackLocal>,
    current_quality: Option<StreamQuality>,
    current_channel: Option<Addr<RtpPacketGatewayRouter<Self, VideoRouterContext>>>,
    pending_channel: Option<Addr<RtpPacketGatewayRouter<Self, VideoRouterContext>>>,
    timeout_channel: tokio::sync::mpsc::Sender<()>,
    start_instant: Instant,
    ssrc: u32,
    payload_type: u8,
    current_generated_ts: u32,
    last_sequence_number: u16,
    is_layer_switching: bool,
}


impl VideoPacketForwarder {
    pub fn new(track: Arc<dyn TrackLocal>, timeout_channel: tokio::sync::mpsc::Sender<()>, ssrc: u32, payload_type: u8) -> Self {
        Self {
            track,
            timeout_channel,
            last_sequence_number: 0,
            is_layer_switching: false, 
            start_instant: Instant::now(),
            current_generated_ts: 0,
            ssrc,
            payload_type,
            current_quality: None,
            current_channel: None,
            pending_channel: None,
        }
    }
}


pub enum VideoPacketForwarderMessage {
    RtpPacket { packet: Packet, quality: StreamQuality },
    Timeout,
    Reset,
    Start { quality: StreamQuality, gateway_router: Addr<RtpPacketGatewayRouter<VideoPacketForwarder, VideoRouterContext>> },
    LayerSwitched { quality: StreamQuality, gateway_router: Addr<RtpPacketGatewayRouter<VideoPacketForwarder, VideoRouterContext>> },
    MissedPackets(Vec<u16>)
}

impl From<RtpPacketMessage> for VideoPacketForwarderMessage {
    fn from(message: RtpPacketMessage) -> Self {
        match message {
            RtpPacketMessage::Packet(quality, packet) => VideoPacketForwarderMessage::RtpPacket { packet, quality },
            RtpPacketMessage::Timeout => VideoPacketForwarderMessage::Timeout
        }
    }
}

impl VideoPacketForwarder {
    async fn forward(&mut self, ctx: &Ctx<'_, Self>, quality: StreamQuality, mut packet: Packet) {
        if self.is_layer_switching && self.current_quality != Some(quality) {
            self.current_quality = Some(quality);
            self.is_layer_switching = false;
            if let Some(pending_channel) = self.pending_channel.take() {
                if let Some(cancelled_channel) = self.current_channel.replace(pending_channel) {
                    let _ = cancelled_channel.do_send(RtpPacketGatewayRouterMessage::Unsubscribe(ctx.addr.clone()));
                }
            }
        }
        if self.current_quality == Some(quality) {
            self.modify_header(&mut packet);
            self.write_packet(packet).await;
        }
    }
    async fn write_packet(&mut self, packet: Packet)  {
        match self.track.write_rtp(packet).await {
            Ok(_) => {},
            Err(e) => {
                tracing::error!("{e}");
            } 
        }
    }
    async fn handle_missed_packets(&mut self, packets: Vec<u16>) {

    }
    fn modify_header(&mut self, packet: &mut Packet) {
        self.last_sequence_number = self.last_sequence_number.wrapping_add(1);
        packet.header.payload_type = self.payload_type;
        packet.header.ssrc = self.ssrc;
        packet.header.sequence_number = self.last_sequence_number;
        packet.header.timestamp = self.current_generated_ts;
        if packet.header.marker {
            let elapsed_micros = self.start_instant.elapsed().as_micros() as u64;
            let target_timestamp = elapsed_micros.wrapping_mul(9) / 100;
            self.current_generated_ts = target_timestamp as u32;
        }
    }
}

impl Actor for VideoPacketForwarder {
    type Message = VideoPacketForwarderMessage;
    async fn starting(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
    }
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            VideoPacketForwarderMessage::Reset => {
                if self.is_layer_switching { return; }
                if let Some(current_channel) = &self.current_channel {
                    current_channel.do_send(RtpPacketGatewayRouterMessage::Unsubscribe(ctx.addr.clone())).ok_or_terminate(ctx);
                    current_channel.do_send(RtpPacketGatewayRouterMessage::Subscribe(ctx.addr.clone())).ok_or_terminate(ctx);
                }
            }
            VideoPacketForwarderMessage::LayerSwitched {
                quality,
                gateway_router: forwarder
            } =>
            if !self.is_layer_switching && self.current_quality != Some(quality) {
                self.is_layer_switching = true;
                forwarder.do_send(RtpPacketGatewayRouterMessage::Subscribe(ctx.addr.clone())).ok_or_terminate(ctx);
                self.pending_channel = Some(forwarder);
            },
            VideoPacketForwarderMessage::Start { quality, gateway_router: forwarder } => {
                self.current_quality = Some(quality);
                forwarder.do_send(RtpPacketGatewayRouterMessage::Subscribe(ctx.addr.clone())).ok_or_terminate(ctx);
                self.current_channel = Some(forwarder);
            }
            VideoPacketForwarderMessage::RtpPacket { packet, quality} => {
                self.forward(ctx, quality, packet).await;
            },
            VideoPacketForwarderMessage::Timeout => {
                self.timeout_channel.send(()).await.map_err(|_| Error::ChannelClosed).ok_or_terminate(ctx);
            }
            VideoPacketForwarderMessage::MissedPackets(missed_packets) => { 
                self.handle_missed_packets(missed_packets).await;
            }
        }
    }
    async fn stopping(mut self, ctx: &crate::actor::Ctx<'_, Self>) {
        if let Some(channel) = self.current_channel.take() {
            let _ = channel.send(RtpPacketGatewayRouterMessage::Unsubscribe(ctx.addr.clone())).await;
        }
        if let Some(channel) = self.pending_channel.take() {
            let _ = channel.send(RtpPacketGatewayRouterMessage::Unsubscribe(ctx.addr.clone())).await;
        }
    }
}