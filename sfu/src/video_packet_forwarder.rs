use std::{sync::Arc, time::{Instant}};
use webrtc::{rtp::{packet::Packet}, track::track_local::{TrackLocalWriter, track_local_static_rtp::TrackLocalStaticRTP}};
use crate::{actor::{Actor, Addr, Ctx, StoppingExt}, error::Error, room::StreamQuality, rtp_packet_gateway_router::{RtpPacketGatewayRouter, RtpPacketGatewayRouterMessage, RtpPacketMessage, VideoRouterContext}, video_layer_manager::{VideoLayerManager, VideoLayerManagerMessage}};

pub struct VideoPacketForwarder {
    track: Arc<TrackLocalStaticRTP>,
    video_layer_manager: Addr<VideoLayerManager>,
    current_quality: Option<StreamQuality>,
    current_channel: Option<Addr<RtpPacketGatewayRouter<Self, VideoRouterContext>>>,
    pending_channel: Option<Addr<RtpPacketGatewayRouter<Self, VideoRouterContext>>>,
    rtp_packet_cache: RtpPacketCache,
    start_instant: Instant,
    last_packet_time: Instant,
    current_generated_ts: u32,
    last_sequence_number: u16,
    is_layer_switching: bool,
}
pub struct RtpPacketCache {
    inner: Box<[Option<Packet>; u16::MAX as usize + 1]>
}

impl Default for RtpPacketCache {
    fn default() -> Self {
        let cache = vec![None; u16::MAX as usize + 1];
        let inner = cache.into_boxed_slice().try_into().unwrap();
        Self {
            inner,
        }
    }
}

impl RtpPacketCache {
    fn get(&self, index: u16) -> Option<&Packet> {
        self.inner[index as usize].as_ref()
    }
    fn insert(&mut self, packet: Packet) {
        let index = packet.header.sequence_number as usize;
        self.inner[index] = Some(packet);
    }
    fn reset(&mut self) {
        let cache = vec![None; u16::MAX as usize + 1];
        self.inner = cache.into_boxed_slice().try_into().unwrap();
    }
}

impl VideoPacketForwarder {
    pub fn new(track: Arc<TrackLocalStaticRTP>, video_layer_manager: Addr<VideoLayerManager>) -> Self {
        Self {
            track,
            last_sequence_number: 0,
            rtp_packet_cache: RtpPacketCache::default(),
            is_layer_switching: false, 
            start_instant: Instant::now(),
            last_packet_time: Instant::now(),
            current_generated_ts: 0,
            current_quality: None,
            current_channel: None,
            pending_channel: None,
            video_layer_manager,
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
    async fn handle_missed_packets(&mut self, missing_packets: Vec<u16>) -> Result<(), Error> {
        for packet_number in missing_packets {
            if let Some(packet) = self.rtp_packet_cache.get(packet_number) {
                self.track.write_rtp(&packet).await?;
            }
        }
        Ok(())
    }
    async fn write_packet(&mut self, packet: Packet)  {
        match self.track.write_rtp(&packet).await {
            Ok(_) => {},
            Err(e) => {
                tracing::warn!("{e}");
            } 
        }
        self.rtp_packet_cache.insert(packet);
    }
    fn modify_header(&mut self, packet: &mut Packet) {
        self.last_sequence_number = self.last_sequence_number.wrapping_add(1);
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
            VideoPacketForwarderMessage::RtpPacket {packet, quality} => {
                self.forward(ctx, quality, packet).await;
                self.last_packet_time = Instant::now();
            },
            VideoPacketForwarderMessage::Timeout => {
                let message= VideoLayerManagerMessage::FallbackToLowQuality;
                self.video_layer_manager
                    .send(message)
                    .await
                    .ok_or_terminate(ctx);
            }
            VideoPacketForwarderMessage::MissedPackets(missed_packets) => { 
                self.handle_missed_packets(missed_packets).await.ok_or_terminate(ctx);
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
        tracing::info!("[VideoPacketForwarder] Stopping");
    }
}