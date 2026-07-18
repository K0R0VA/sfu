use std::{sync::Arc, time::{Instant}};
use webrtc::{rtp::{packet::Packet}, track::track_local::{TrackLocalWriter, track_local_static_rtp::TrackLocalStaticRTP}};
use crate::{actor::{Actor, Addr, Ctx, StoppingExt}, error::Error, room::{MimeType, StreamQuality}, rtp_packet_gateway_router::{RtpPacketGatewayRouter, RtpPacketGatewayRouterMessage}};

pub struct VideoPacketForwarder {
    track: Arc<TrackLocalStaticRTP>,
    mime_type: MimeType,
    current_quality: Option<StreamQuality>,
    current_channel: Option<Addr<RtpPacketGatewayRouter<Self>>>,
    pending_channel: Option<Addr<RtpPacketGatewayRouter<Self>>>,
    key_frame_buffer: Vec<Packet>,
    rtp_packet_cache: RtpPacketCache,
    start_instant: Instant,
    last_incoming_ts: u32,            
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
}

impl VideoPacketForwarder {
    pub fn new(track: Arc<TrackLocalStaticRTP>, mime_type: MimeType) -> Self {
        Self {
            track,
            mime_type,
            last_sequence_number: u16::MAX - 1000,
            key_frame_buffer: Vec::new(),
            rtp_packet_cache: RtpPacketCache::default(),
            is_layer_switching: false, 
            start_instant: Instant::now(),
            current_generated_ts: 0,
            last_incoming_ts: 0,
            current_quality: None,
            current_channel: None,
            pending_channel: None,
        }
    }
}


pub enum VideoPacketForwarderMessage {
    RtpPacket { packet: Packet, quality: StreamQuality },
    Start { quality: StreamQuality, gateway_router: Addr<RtpPacketGatewayRouter<VideoPacketForwarder>> },
    LayerSwitched {gateway_router: Addr<RtpPacketGatewayRouter<VideoPacketForwarder>> },
    MissedPackets(Vec<u16>)
}

impl From<(StreamQuality, Packet)> for VideoPacketForwarderMessage {
    fn from((quality, packet): (StreamQuality, Packet)) -> Self {
        VideoPacketForwarderMessage::RtpPacket { packet, quality }
    }
}

impl VideoPacketForwarder {
    async fn forward(&mut self, ctx: &Ctx<'_, Self>, quality: StreamQuality, mut packet: Packet) -> Result<(), Error> {
        if self.is_layer_switching && self.current_quality != Some(quality) {
            self.handle_pending_packets(ctx, quality, packet).await?;
            return Ok(());
        }
        if self.current_quality == Some(quality) {
            self.apply_offsets(&mut packet);
            self.write_packet(packet).await?;
        }
        Ok(())
    }
    async fn handle_missed_packets(&mut self, missing_packets: Vec<u16>) -> Result<(), Error> {
        for packet_number in missing_packets {
            if let Some(packet) = self.rtp_packet_cache.get(packet_number) {
                self.track.write_rtp(&packet).await?;
            }
        }
        Ok(())
    }
    async fn write_packet(&mut self, packet: Packet) -> Result<(), Error> {
        self.track.write_rtp(&packet).await?;
        self.rtp_packet_cache.insert(packet);
        Ok(())
    }
    async fn handle_pending_packets(&mut self, ctx: &Ctx<'_, Self>, quality: StreamQuality, mut packet: Packet) -> Result<(), Error> {
        let is_key_frame_start = match self.mime_type {
            MimeType::H264 => is_h264_key_frame(&packet.payload),
            MimeType::VP8 => is_vp8_key_frame(&packet),
            MimeType::VP9 => is_vp9_key_frame(&packet.payload),
            MimeType::Audio(_) => false
        };
        let keyframe_started = !self.key_frame_buffer.is_empty();
        if is_key_frame_start || keyframe_started {
            let is_key_frame_end = packet.header.marker;
            if !is_key_frame_end {
                self.key_frame_buffer.push(packet);
                return Ok(());
            }
            self.current_quality = Some(quality);
            self.is_layer_switching = false;
            if let Some(pending_channel) = self.pending_channel.take() {
                if let Some(cancelled_channel) = self.current_channel.replace(pending_channel) {
                    let _ = cancelled_channel.do_send(RtpPacketGatewayRouterMessage::Unsubscribe(ctx.addr.clone()));
                }
            }
            let mut buffer = Vec::with_capacity(self.key_frame_buffer.len());
            std::mem::swap(&mut buffer, &mut self.key_frame_buffer);
            for mut packet in buffer {
                self.apply_offsets(&mut packet);
                self.write_packet(packet).await?;
            }
            self.apply_offsets(&mut packet);
            self.write_packet(packet).await?;
        }
        Ok(())
    }
    fn apply_offsets(&mut self, packet: &mut Packet) {
        self.last_sequence_number = self.last_sequence_number.wrapping_add(1);
        packet.header.sequence_number = self.last_sequence_number;
        if self.last_incoming_ts != packet.header.timestamp {
            let elapsed_micros = self.start_instant.elapsed().as_micros() as u64;
            let target_timestamp = elapsed_micros.wrapping_mul(9) / 100;
            self.last_incoming_ts = packet.header.timestamp;
            self.current_generated_ts = target_timestamp as u32;
        }
        packet.header.timestamp = self.current_generated_ts;
    }
}

impl Actor for VideoPacketForwarder {
    type Message = VideoPacketForwarderMessage;
    async fn starting(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[VideoPacketForwarder] Starting");
    }
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            VideoPacketForwarderMessage::LayerSwitched {
                gateway_router: forwarder
            } 
            if !self.is_layer_switching => {
                self.is_layer_switching = true;
                forwarder.do_send(RtpPacketGatewayRouterMessage::Subscribe(ctx.addr.clone())).ok_or_terminate(ctx);
                self.pending_channel = Some(forwarder);
            },
            VideoPacketForwarderMessage::Start { quality, gateway_router: forwarder } => {
                self.current_quality = Some(quality);
                forwarder.do_send(RtpPacketGatewayRouterMessage::Subscribe(ctx.addr.clone())).ok_or_terminate(ctx);
                self.current_channel = Some(forwarder);
                self.start_instant = Instant::now();
            }
            VideoPacketForwarderMessage::RtpPacket {packet, quality} => {
                self.forward(ctx, quality, packet).await.ok_or_terminate(ctx);
            },
            VideoPacketForwarderMessage::MissedPackets(missed_packets) =>{ 
                self.handle_missed_packets(missed_packets).await.ok_or_terminate(ctx);
            }
            _ => {}
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

fn is_vp8_key_frame(packet: &Packet) -> bool {
    let payload = packet.payload.as_ref();
    if payload.is_empty() { return false; }
    let mut header_index = 0;
    if payload[0] & 0x80 != 0 { // X bit set
        header_index += 1;
        if header_index >= payload.len() { return false; }
        
        let has_i = payload[header_index] & 0x80 != 0;
        let has_l = payload[header_index] & 0x40 != 0;
        let has_t = payload[header_index] & 0x20 != 0;
        let has_k = payload[header_index] & 0x10 != 0;
        
        header_index += 1; 
        if has_i { header_index += 1; if header_index < payload.len() && payload[header_index-1] & 0x80 != 0 { header_index += 1; } } // PictureID
        if has_l { header_index += 1; } 
        if has_t || has_k { header_index += 1; }
    } else {
        header_index += 1;
    }

    if header_index >= payload.len() { return false; }
    let vp8_payload_header = payload[header_index];
    (vp8_payload_header & 0x01) == 0
}

fn is_h264_key_frame(payload: &[u8]) -> bool {
    if payload.is_empty() {
        return false;
    }
    
    // Первый байт - это обычно NAL header
    let nal_header = payload[0];
    let nal_type = nal_header & 0x1F;
    
    // Single NAL Unit packet (типы 1-23)
    if nal_type >= 1 && nal_type <= 23 {
        // NAL type 5 = IDR slice (ключевой кадр)
        // NAL type 7 = SPS (Sequence Parameter Set)
        // NAL type 8 = PPS (Picture Parameter Set)
        return nal_type == 5;
    }
    
    // FU-A или FU-B (Fragmentation Unit)
    if nal_type == 28 || nal_type == 29 {
        if payload.len() < 2 {
            return false;
        }
        
        // FU header: второй байт
        let fu_header = payload[1];
        let start_bit = (fu_header & 0x80) != 0;
        let fu_type = fu_header & 0x1F;
        
        // Ключевой кадр - это IDR (type 5) и это начало фрагмента
        // (или единственный фрагмент, где start и end bits оба установлены)
        return fu_type == 5 && start_bit;
    }
    
    // STAP-A (Aggregation packet) - тип 24
    if nal_type == 24 {
        let mut offset = 1;
        while offset + 2 < payload.len() {
            // Размер NAL блока (2 байта, big-endian)
            let nal_size = ((payload[offset] as usize) << 8) | (payload[offset + 1] as usize);
            offset += 2;
            
            if offset + nal_size > payload.len() {
                break;
            }
            
            // Проверяем NAL внутри агрегации
            let nal_header = payload[offset];
            let inner_nal_type = nal_header & 0x1F;
            
            if inner_nal_type == 5 {
                return true; // Нашли ключевой кадр
            }
            
            offset += nal_size;
        }
        return false;
    }
    
    false
}

fn is_vp9_key_frame(payload: &[u8]) -> bool {
    if payload.is_empty() { return false; }
    
    // Parse VP9 payload descriptor (RFC 7741)
    let mut offset = 0;
    let byte = payload[offset];
    
    offset += 1;
    
    // Check for extended descriptor
    if (byte & 0x80) != 0 {
        // Extended descriptor, skip more
        if offset >= payload.len() { return false; }
        let extended = payload[offset];
        offset += 1;
        
        if (extended & 0x80) != 0 { // Z bit - show frame
            // Additional parsing might be needed
        }
    }
    
    // Check VP9 frame marker (last 2 bits of first byte of frame header)
    if offset >= payload.len() { return false; }
    let frame_marker = payload[offset] & 0x03;
    if frame_marker != 0x02 { // Invalid frame marker
        return false;
    }
    
    // Profile and bit to detect key frame
    // For simplicity: key frames have frame_type = 0 (bit 0 of byte after marker)
    if offset + 1 >= payload.len() { return false; }
    let frame_type = (payload[offset + 1] >> 6) & 0x01;
    
    frame_type == 0 // 0 = key frame, 1 = inter frame
}