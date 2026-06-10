use std::{sync::Arc, time::{Duration, Instant}};

use tokio::time::{interval};
use webrtc::{peer_connection::RTCPeerConnection, stats::StatsReportType};

use crate::{actor::{Actor, Addr, StreamItem}, room::StreamQuality, user::{User, UserMessage}};

pub struct QualityMonitor {
    pc: Arc<RTCPeerConnection>,
    user: Addr<User>,
    packet_loss: f64,
    bitrate_bps: u64,
    last_packets_received: u64,
    last_bytes_received: u64,
    last_nack_count: u64,
    last_stats_time: Instant,
    current_quality: StreamQuality, 
    consecutive_high_signals: usize, 
}

#[derive(Clone, Copy)]
pub enum QualityMonitorMessage {
    Ping, 
    Close
}

impl Actor for QualityMonitor {
    type Message = QualityMonitorMessage;
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, m: Self::Message) {
        match m {
            QualityMonitorMessage::Ping  => {
                self.update_stats().await;
                if let Some(quality) = self.get_quality() {
                    let should_switch = match quality.cmp(&self.current_quality) {
                        std::cmp::Ordering::Equal => {
                            self.consecutive_high_signals = 0;
                            false
                        },
                        std::cmp::Ordering::Greater if self.consecutive_high_signals > 3 => {
                            self.consecutive_high_signals = 0;
                            true
                        }
                        std::cmp::Ordering::Greater => {
                            self.consecutive_high_signals += 1;
                            false
                        },
                        std::cmp::Ordering::Less => {
                            self.consecutive_high_signals = 0;
                            true
                        }
                    };
                    if should_switch {
                        self.current_quality = quality;
                        let _ = self.user
                            .send(UserMessage::SwitchQualityLayer { quality })
                            .await;
                    }
                }
            }
            QualityMonitorMessage::Close => {
                self.stop(ctx).await;
            }
        }
        
    }
    async fn starting(&mut self, ctx: &crate::actor::Ctx<'_, Self>) {
        let stream = tokio_stream::wrappers::IntervalStream::new(interval(Duration::from_secs(3)));
        ctx.addr.add_stream(stream, |_| StreamItem::Next(QualityMonitorMessage::Ping));
        tracing::info!("[QualityMonitor] starting")
    }
    async fn stopping(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[QualityMonitor] Stopping")
    }
}

impl QualityMonitor {
    pub fn new(pc: Arc<RTCPeerConnection>, user: Addr<User>) -> Self {
        Self {
            pc,
            user,
            bitrate_bps: 0,
            packet_loss: 0.,
            last_bytes_received: 0,
            last_nack_count: 0,
            last_packets_received: 0,
            last_stats_time: Instant::now(),
            current_quality: StreamQuality::High,
            consecutive_high_signals: 0
        }
    }
    async fn update_stats(&mut self) {
        let stats = self.pc.get_stats().await;
        let now = std::time::Instant::now();
        
        // Точный расчет интервала времени
        let elapsed_secs = now.duration_since(self.last_stats_time).as_secs_f64();
        self.last_stats_time = now;
        let mut total_bytes_received: u64 = 0;
        let mut total_packets_received: u64 = 0;
        let mut total_nack_count: u64 = 0;
        for report in stats.reports.values() {
            match report {
                StatsReportType::InboundRTP(inbound) => {
                    if inbound.kind == "video" {
                        total_bytes_received += inbound.bytes_received;
                        total_packets_received += inbound.packets_received;
                        total_nack_count += inbound.nack_count;
                    }
                }
                _ => {}
            }
        }
        if self.last_bytes_received > 0 && total_bytes_received > self.last_bytes_received && elapsed_secs > 0.0 {
            let bytes_diff = total_bytes_received.saturating_sub(self.last_bytes_received);
            self.bitrate_bps = ((bytes_diff * 8) as f64 / elapsed_secs) as u64;
        }
        
        if self.last_packets_received > 0 && total_packets_received > self.last_packets_received {
            let packets_diff = total_packets_received.saturating_sub(self.last_packets_received);
            let nack_diff = total_nack_count.saturating_sub(self.last_nack_count);
            
            if packets_diff > 0 {
                // Защита: nack_diff может быть больше packets_diff при сильном спурте.
                // Ограничиваем отношение сверху единицей (100%), чтобы не ломать логику.
                let loss_ratio = (nack_diff as f64 / packets_diff as f64).min(1.0);
                
                // ВАЖНО: Так как NACK — это не чистые потери (часть пакетов восстанавливается), 
                // мы делим полученный коэффициент на 2, чтобы получить более реалистичную оценку "потерь".
                // Иначе из-за дублирующих NACK-ов алгоритм будет слишком агрессивно дропать качество.
                self.packet_loss = (loss_ratio * 100.0) / 2.0;
            }
        } else if total_packets_received == self.last_packets_received && self.last_packets_received > 0 {
            // Если пакеты вообще перестали приходить, считаем это 100% потерей связи
            self.packet_loss = 100.0;
        }
        self.last_bytes_received = total_bytes_received;
        self.last_packets_received = total_packets_received;
        self.last_nack_count = total_nack_count;
    }
    fn get_quality(&self) -> Option<StreamQuality> {
        if self.last_bytes_received == 0 {
            return Some(StreamQuality::High);
        }
        if self.packet_loss > 15.0 || self.bitrate_bps < 150_000 {
            tracing::info!("{} {}", self.packet_loss, self.bitrate_bps);
            return Some(StreamQuality::Low);
        }
        if self.packet_loss > 8.0 || self.bitrate_bps < 350_000 {
            return Some(StreamQuality::Mid);
        }
        if self.bitrate_bps > 1_200_000 && self.packet_loss < 2.0 {
            return Some(StreamQuality::High);
        }
        None
    }
}