use std::{sync::Arc, time::{Duration, Instant}};

use serde::{Deserialize, Serialize};
use tokio::time::{interval};
use webrtc::{peer_connection::RTCPeerConnection, stats::StatsReportType};

use crate::{actor::{Actor, Addr, StreamItem}, room::StreamQuality, user::{User, UserMessage}};

pub struct QualityMonitor {
    pc: Arc<RTCPeerConnection>,
    user: Addr<User>,
    last_stats_time: Instant,
    consecutive_high_signals: usize, 
    current_quality: Option<StreamQuality>, 
    current_stats: CurrentStats,
    thresholds: QualityThresholds
}

#[derive(Default)]
pub struct CurrentStats {
    packet_loss: f64,
    bitrate_bps: u64,
    last_packets_received: u64,
    last_bytes_received: u64,
    last_nack_count: u64,
}
pub struct QualityThresholds {
    low_bitrate: u64,
    mid_bitrate: u64,
    high_bitrate: u64,
    low_loss: f64,
    mid_loss: f64,
    high_loss: f64,
}

impl From<DeviceType> for QualityThresholds {
    fn from(value: DeviceType) -> Self {
        match value {
            DeviceType::Desktop => QualityThresholds {
                low_bitrate: 150_000,
                mid_bitrate: 350_000,
                high_bitrate: 700_00,
                low_loss: 15.0,
                mid_loss: 8.0,
                high_loss: 2.0,
            },
            DeviceType::Mobile => QualityThresholds {
                low_bitrate: 75_000,
                mid_bitrate: 150_000,
                high_bitrate: 350_000,
                low_loss: 15.0,
                mid_loss: 8.0,
                high_loss: 2.0,
            },
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum DeviceType {
    Desktop,
    Mobile,
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
                    let current_quality = match self.current_quality {
                        Some(current_quality) => current_quality,
                        None => {
                            self.current_quality = Some(quality);
                            let _ = self.user
                                .send(UserMessage::SwitchQualityLayer { quality })
                                .await;
                            return;
                        }
                    };
                    let should_switch = match quality.cmp(&current_quality) {
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
                            true
                        },
                        std::cmp::Ordering::Less => {
                            self.consecutive_high_signals = 0;
                            true
                        }
                    };
                    if should_switch {
                        tracing::info!("[QualityMonitor] {:?} last_bytes_received {} bitrate_bps {} packet_loss {}", quality, self.current_stats.last_bytes_received, self.current_stats.bitrate_bps, self.current_stats.packet_loss);
                        self.current_quality = Some(quality);
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
        let stream = tokio_stream::wrappers::IntervalStream::new(interval(Duration::from_secs(30)));
        ctx.addr.add_stream(stream, |_| StreamItem::Next(QualityMonitorMessage::Ping));
        tracing::info!("[QualityMonitor] starting")
    }
    async fn stopping(self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[QualityMonitor] Stopping")
    }
}

impl QualityMonitor {
    pub fn new(pc: Arc<RTCPeerConnection>, user: Addr<User>, thresholds: QualityThresholds) -> Self {
        Self {
            pc,
            user,
            current_stats: CurrentStats::default(),
            thresholds,
            last_stats_time: Instant::now(),
            current_quality: None,
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
        if self.current_stats.last_bytes_received > 0 && total_bytes_received > self.current_stats.last_bytes_received && elapsed_secs > 0.0 {
            let bytes_diff = total_bytes_received.saturating_sub(self.current_stats.last_bytes_received);
            self.current_stats.bitrate_bps = ((bytes_diff * 8) as f64 / elapsed_secs) as u64;
        }
        if self.current_stats.last_packets_received > 0 && total_packets_received > self.current_stats.last_packets_received {
            let packets_diff = total_packets_received.saturating_sub(self.current_stats.last_packets_received);
            let nack_diff = total_nack_count.saturating_sub(self.current_stats.last_nack_count);
            if packets_diff > 0 {
                let loss_ratio = (nack_diff as f64 / packets_diff as f64).min(1.0);
                self.current_stats.packet_loss = (loss_ratio * 100.0) / 2.0;
            }
        } else if total_packets_received == self.current_stats.last_packets_received && self.current_stats.last_packets_received > 0 {
            // Если пакеты вообще перестали приходить, считаем это 100% потерей связи
            self.current_stats.packet_loss = 100.0;
        }
        self.current_stats.last_bytes_received = total_bytes_received;
        self.current_stats.last_packets_received = total_packets_received;
        self.current_stats.last_nack_count = total_nack_count;
    }
    fn get_quality(&self) -> Option<StreamQuality> {
        if self.current_stats.bitrate_bps == 0 {
            return Some(StreamQuality::High);
        }
        if self.current_stats.packet_loss > self.thresholds.low_loss || self.current_stats.bitrate_bps < self.thresholds.low_bitrate {
            return Some(StreamQuality::Low);
        }
        if self.current_stats.packet_loss > self.thresholds.mid_loss || self.current_stats.bitrate_bps < self.thresholds.mid_bitrate {
            return Some(StreamQuality::Mid);
        }
        if self.current_stats.packet_loss < self.thresholds.high_loss && self.current_stats.bitrate_bps > self.thresholds.high_bitrate {
            return Some(StreamQuality::High);
        }
        None
    }
}