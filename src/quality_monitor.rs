use std::{sync::Arc, time::Duration};

use tokio::time::{interval};
use webrtc::{peer_connection::RTCPeerConnection, stats::StatsReportType};

use crate::{actor::{Actor, Addr, StreamItem}, room::StreamQuality, video_subscription::{VideoSubscription, VideoSubscriptionMessage}};

pub struct QualityMonitor {
    pc: Arc<RTCPeerConnection>,
    subscription: Addr<VideoSubscription>,
    packet_loss: f64,
    bitrate_bps: u64,
    last_packets_received: u64,
    last_bytes_received: u64,
    last_nack_count: u64
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
                if let Some(quality) = self.get_current_quality() {
                    let Err(_) = self.subscription
                        .send(VideoSubscriptionMessage::SwitchQualityLayer { to: quality })
                        .await else { return; };
                    self.stop(ctx).await;
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            QualityMonitorMessage::Close => {
                self.stop(ctx).await;
            }
        }
        
    }
    async fn starting(&mut self, ctx: &crate::actor::Ctx<'_, Self>) {
        let stream = tokio_stream::wrappers::IntervalStream::new(interval(Duration::from_secs(5)));
        ctx.addr.add_stream(stream, |_| StreamItem::Next(QualityMonitorMessage::Ping));
        tracing::info!("[QualityMonitor] starting")
    }
    async fn stopping(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[QualityMonitor] Stopping")
    }
}

impl QualityMonitor {
    pub fn new(pc: Arc<RTCPeerConnection>, subscription: Addr<VideoSubscription>) -> Self {
        Self {
            pc,
            subscription,
            bitrate_bps: 0,
            packet_loss: 0.,
            last_bytes_received: 0,
            last_nack_count: 0,
            last_packets_received: 0,
        }
    }
    async fn update_stats(&mut self) {
        let stats = self.pc.get_stats().await;
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
        
        if self.last_bytes_received > 0 {
            let bytes_diff = total_bytes_received.saturating_sub(self.last_bytes_received);
            self.bitrate_bps = bytes_diff * 8 / 2; // бит/сек за 2 секунды
        }
        
        if self.last_packets_received > 0 {
            let packets_diff = total_packets_received.saturating_sub(self.last_packets_received);
            let nack_diff = total_nack_count.saturating_sub(self.last_nack_count);
            
            if packets_diff > 0 {
                self.packet_loss = (nack_diff as f64 / packets_diff as f64) * 100.0;
            }
        }
        
        self.last_bytes_received = total_bytes_received;
        self.last_packets_received = total_packets_received;
        self.last_nack_count = total_nack_count;
    }
    fn get_current_quality(&self) -> Option<StreamQuality> {
        if self.last_bytes_received == 0 || self.last_packets_received == 0 {
            return None;
        }
        if self.packet_loss > 10.0 || self.bitrate_bps < 200_000 {
            return Some(StreamQuality::Low);
        }
    
        if self.packet_loss > 5.0 || self.bitrate_bps < 500_000 {
            return Some(StreamQuality::Mid);
        }
    
        Some(StreamQuality::High)
    }
}