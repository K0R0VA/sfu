use std::sync::{Arc};

use webrtc::{rtp::packet::Packet, track::track_local::{TrackLocalWriter, track_local_static_rtp::TrackLocalStaticRTP}};

use crate::{actor::{Actor, Addr, Ctx},  error::Error, room::{MimeType, StreamQuality}, rtp_packet_forwarder::{RtpPacketForwarder, RtpPacketForwarderMessage}};

pub struct VideoPacketForwarder {
    track: Arc<TrackLocalStaticRTP>,
    mime_type: MimeType,
    current_quality: Option<StreamQuality>,
    current_channel: Option<Addr<RtpPacketForwarder<Self>>>,
    pending_channel: Option<Addr<RtpPacketForwarder<Self>>>,
    last_sequence_number: u16,
    last_timestamp: u32,
    sequence_number_offset: i32,
    timestamp_offset: i32,
    is_layer_switching: bool,
}

impl VideoPacketForwarder {
    pub fn new(track: Arc<TrackLocalStaticRTP>, mime_type: MimeType) -> Self {
        Self {
            track,
            mime_type,
            last_sequence_number: 0,
            last_timestamp: 0,
            sequence_number_offset: 0,
            timestamp_offset: 0,
            // Стартуем в режиме ожидания первого ключевого кадра
            is_layer_switching: false, 
            current_quality: None,
            current_channel: None,
            pending_channel: None,
        }
    }
}


pub enum VideoPacketForwarderMessage {
    RtpPacket { packet: Packet, quality: StreamQuality },
    Start { quality: StreamQuality, forwarder: Addr<RtpPacketForwarder<VideoPacketForwarder>> },
    LayerSwitched { quality: StreamQuality, forwarder: Addr<RtpPacketForwarder<VideoPacketForwarder>> }
}

impl From<(StreamQuality, Packet)> for VideoPacketForwarderMessage {
    fn from((quality, packet): (StreamQuality, Packet)) -> Self {
        VideoPacketForwarderMessage::RtpPacket { packet, quality }
    }
}

impl VideoPacketForwarder {
    async fn forward(&mut self, ctx: &Ctx<'_, Self>, quality: StreamQuality, mut packet: Packet) -> Result<(), Error> {
        if self.is_layer_switching && self.current_quality != Some(quality) {
            self.handle_pending_packets(ctx, quality, &mut packet).await?;
            return Ok(());
        }
        if self.current_quality == Some(quality) {
            self.apply_offsets(&mut packet);
            self.track.write_rtp(&packet).await?;
        }
        Ok(())
    }
    async fn handle_pending_packets(&mut self, ctx: &Ctx<'_, Self>, quality: StreamQuality, mut packet: &mut Packet) -> Result<(), Error> {
        let is_key_frame = match self.mime_type {
            MimeType::H264 => is_h264_key_frame(&packet.payload),
            MimeType::VP8 => is_vp8_key_frame(&packet),
            MimeType::VP9 => is_vp9_key_frame(&packet.payload),
            _ => unreachable!()
        };
        if is_key_frame {
            self.update_offsets_on_layer_switch(&packet);
            self.current_quality = Some(quality);
            self.is_layer_switching = false;
            if let Some(pending_channel) = self.pending_channel.take() {
                if let Some(cancelled_channel) = self.current_channel.replace(pending_channel) {
                    let _ = cancelled_channel.do_send(RtpPacketForwarderMessage::Unsubscribe(ctx.addr.clone()));
                }
            }
            self.apply_offsets(&mut packet);
            self.track.write_rtp(&packet).await?;
        }
        Ok(())
    }
    fn update_offsets_on_layer_switch(&mut self, packet: &Packet) {
        let new_sequence_number = packet.header.sequence_number;
        let new_timestamp = packet.header.timestamp;
        self.sequence_number_offset = (self.last_sequence_number.wrapping_add(1) as i32) 
            - (new_sequence_number as i32);
        self.timestamp_offset = self.last_timestamp as i32 
            - new_timestamp as i32;
    }
    fn apply_offsets(&mut self, packet: &mut Packet) {
        let original_seq = packet.header.sequence_number;
        let original_ts = packet.header.timestamp;
        // Применяем оффсет с учетом wrapping для u16
        let new_seq = (original_seq as i32 + self.sequence_number_offset) as u16;
        let new_ts = (original_ts as i32 + self.timestamp_offset) as u32;
        
        packet.header.sequence_number = new_seq;
        packet.header.timestamp = new_ts;
        
        // Обновляем последние значения
        self.last_sequence_number = new_seq;
        self.last_timestamp = new_ts;
    }
}

impl Actor for VideoPacketForwarder {
    type Message = VideoPacketForwarderMessage;
    async fn starting(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[VideoPacketForwarder] Starting");
    }
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            VideoPacketForwarderMessage::LayerSwitched {quality, forwarder } 
            if !self.is_layer_switching => {
                tracing::info!("[VideoPacketForwarder] LayerSwitched {:?}", quality);
                self.is_layer_switching = true;
                let _ = forwarder.do_send(RtpPacketForwarderMessage::Subscribe(ctx.addr.clone()));
                self.pending_channel = Some(forwarder);
            },
            VideoPacketForwarderMessage::Start { quality, forwarder } => {
                tracing::info!("[VideoPacketForwarder] VideoPacketForwarderMessage::Start");
                self.current_quality = Some(quality);
                let _ = forwarder.do_send(RtpPacketForwarderMessage::Subscribe(ctx.addr.clone()));
                self.current_channel = Some(forwarder);
            }
            VideoPacketForwarderMessage::RtpPacket {packet, quality} => {
                if self.current_quality != Some(quality) && !self.is_layer_switching { 
                    tracing::info!("Old RtpPacket {}", packet.header.timestamp);
                    return; 
                }
                if let Err(e) = self.forward(ctx, quality, packet).await {
                    tracing::error!("[VideoPacketForwarder] write_rtp {e}");
                    self.stop(ctx).await;
                }
            },
            _ => {}
        }
    }
    async fn stopping(mut self, ctx: &crate::actor::Ctx<'_, Self>) {
        if let Some(channel) = self.current_channel.take() {
            let _ = channel.send(RtpPacketForwarderMessage::Unsubscribe(ctx.addr.clone())).await;
        }
        if let Some(channel) = self.pending_channel.take() {
            let _ = channel.send(RtpPacketForwarderMessage::Unsubscribe(ctx.addr.clone())).await;
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
        let end_bit = (fu_header & 0x40) != 0;
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
    
    // Check if it's a key frame
    // Profile + bit 6-7? Actually need to parse VP9 frame header
    // Simplified: key frames have picture_id flag and specific pattern
    let f = (byte >> 4) & 0x03; // F bit?
    
    // Skip payload descriptor
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