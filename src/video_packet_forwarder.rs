use std::{sync::Arc};

use webrtc::{rtp::packet::Packet, track::track_local::{TrackLocalWriter, track_local_static_rtp::TrackLocalStaticRTP}};

use crate::{actor::{Actor},  error::Error};

pub struct VideoPacketForwarder {
    pub track: Arc<TrackLocalStaticRTP>,
    last_sequence_number: u16,
    last_timestamp: u32,
    sequence_number_offset: i32,
    timestamp_offset: i32,
    is_layer_switched: bool,
    awaiting_key_frame: bool,
}

impl VideoPacketForwarder {
    pub fn new(track: Arc<TrackLocalStaticRTP>) -> Self {
        Self {
            track,
            last_sequence_number: 0,
            last_timestamp: 0,
            sequence_number_offset: 0,
            timestamp_offset: 0,
            // Стартуем в режиме ожидания первого ключевого кадра
            is_layer_switched: false, 
            awaiting_key_frame: false,
        }
    }
}

pub enum VideoPacketForwarderMessage {
    RtpPacket (Packet),
    LayerSwitched
}

impl VideoPacketForwarder {
    async fn forward(&mut self, mut packet: Packet) -> Result<(), Error> {
        if self.is_layer_switched {
            self.update_offsets_on_layer_switch(&packet);
            self.is_layer_switched = false;
            self.awaiting_key_frame = true;
            return Ok(());
        }
        if self.awaiting_key_frame {
            if is_key_frame(&packet) {
                self.awaiting_key_frame = false;
            } else {
                return Ok(());
            }
        }
        self.apply_offsets(&mut packet);
        // Отправляем модифицированный пакет в WebRTC трек зрителю
        self.track.write_rtp(&packet).await?;
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
        tracing::info!("[PacketForwarder] Starting");
    }
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            VideoPacketForwarderMessage::LayerSwitched => {
                self.is_layer_switched = true;
            },
            VideoPacketForwarderMessage::RtpPacket(packet) => {
                if let Err(e) = self.forward(packet).await {
                    tracing::error!("[PacketForwarder] write_rtp {e}");
                    self.stop(ctx).await;
                }
            }
        }
    }
    async fn stopping(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[PacketForwarder] Stopping");
    }
}

fn is_key_frame(packet: &Packet) -> bool {
    let payload = packet.payload.as_ref();
    if payload.is_empty() { return false; }

    // Пример для VP8 (RFC 7741):
    // 1. Парсим VP8 Payload Descriptor (может занимать от 1 до 6 байт)
    // Ищем, где заканчивается дескриптор и начинается VP8 Payload Header
    let mut header_index = 0;
    if payload[0] & 0x80 != 0 { // X bit set
        header_index += 1;
        if header_index >= payload.len() { return false; }
        
        let has_i = payload[header_index] & 0x80 != 0;
        let has_l = payload[header_index] & 0x40 != 0;
        let has_t = payload[header_index] & 0x20 != 0;
        let has_k = payload[header_index] & 0x10 != 0;
        
        header_index += 1; // Пропускаем байт расширений
        if has_i { header_index += 1; if header_index < payload.len() && payload[header_index-1] & 0x80 != 0 { header_index += 1; } } // PictureID
        if has_l { header_index += 1; } // TL0PICIDX
        if has_t || has_k { header_index += 1; } // TID/Y/KEYIDX
    } else {
        header_index += 1;
    }

    if header_index >= payload.len() { return false; }

    // 2. Читаем VP8 Payload Header (1-й байт после дескриптора)
    // Бит S (0-й бит) определяет тип кадра: 0 - Key Frame, 1 - Inter Frame
    let vp8_payload_header = payload[header_index];
    (vp8_payload_header & 0x01) == 0
}