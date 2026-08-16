use std::{sync::Arc, time::{Duration, Instant}};

use chrono::Utc;
use rtc::statistics::{StatsSelector, stats::RTCStatsType};
use serde::{Deserialize, Serialize};
use tokio::time::{interval};
use tokio_stream::wrappers::IntervalStream;
use uuid::Uuid;
use webrtc::peer_connection::{PeerConnection, RTCStatsReport, RTCStatsReportEntry};

use crate::{CurrentStats, SignalingClient, Storage, StorageConfiguration, actor::{Actor, Addr, StoppingExt, StreamItem}, error::Error, room::StreamQuality, user::{User, UserMessage}};

pub struct QualityMonitor<C: SignalingClient, S: Storage> {
    id: Uuid,
    user: Addr<User<C, S>>,
    pc: Arc<dyn PeerConnection>,
    storage: S,
    last_stats_time: Instant,
    consecutive_high_signals: usize, 
    current_quality: Option<StreamQuality>, 
    current_stats: CurrentStats,
    thresholds: QualityThresholds,
    update_period: Duration,
}

#[derive(Clone, Copy)]
pub enum QualityMonitorMessage {
    Ping, 
    Close
}

impl<C: SignalingClient, S: Storage> Actor for QualityMonitor<C, S> {
    type Message = QualityMonitorMessage;
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, m: Self::Message) {
        match m {
            QualityMonitorMessage::Ping  => {
                self.update_status().await.ok_or_terminate(ctx);
            }
            QualityMonitorMessage::Close => {
                self.stop(ctx).await;
            }
        }
    }
    async fn starting(&mut self, ctx: &crate::actor::Ctx<'_, Self>) {
        let stream = IntervalStream::new(interval(self.update_period));
        ctx.addr.add_stream(stream, |_| StreamItem::Next(QualityMonitorMessage::Ping));
    }
    async fn stopping(self, _ctx: &crate::actor::Ctx<'_, Self>) {}
}

impl<C: SignalingClient, S: Storage> QualityMonitor<C, S> {
    pub async fn new(pc: Arc<dyn PeerConnection>, user: Addr<User<C, S>>, thresholds: QualityThresholds) -> Result<Self, Error> {
        let configuration = <S::Configuration>::from_env()
            .map_err(|e| Error::SystemError { message: e.to_string().into() })?;
        let storage = S::connect(&configuration).await.map_err(|e| Error::SystemError { message: e.to_string().into() })?;
        Ok(Self {
            id: Uuid::new_v4(),
            user,
            pc,
            storage,
            current_stats: CurrentStats::default(),
            thresholds,
            last_stats_time: Instant::now(),
            current_quality: None,
            consecutive_high_signals: 0,
            update_period: Duration::from_secs(1)
        })
    }
    pub async fn update_status(&mut self) -> Result<(), Error> {
        const REQUIRED_STABLE_SIGNALS: usize = 3;
        self.update_stats().await;
        self.save_stats().await?;
        if let Some(quality) = self.get_quality() {
            let current_quality = match self.current_quality {
                Some(current_quality) => current_quality,
                None => {
                    self.current_quality = Some(quality);
                    self.user
                        .send(UserMessage::SwitchQualityLayer { quality })
                        .await?;
                    return Ok(());
                }
            };
            let should_switch = match quality.cmp(&current_quality) {
                std::cmp::Ordering::Equal => {
                    false
                },
                std::cmp::Ordering::Greater if self.consecutive_high_signals > REQUIRED_STABLE_SIGNALS => {
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
                self.current_quality = Some(quality);
                self.user
                    .send(UserMessage::SwitchQualityLayer { quality })
                    .await?;
            }
        }
        Ok(())
    }
    async fn update_stats(&mut self) {
        let now = Instant::now();
        let stats = self.pc.get_stats(self.last_stats_time, StatsSelector::None).await;
        self.collect_video_metrics(&stats);
        self.calculate_loss_rate();
        self.last_stats_time = now;
    }

    // Сбор всех метрик
    fn collect_video_metrics(&mut self, stats: &RTCStatsReport) {
        let mut total_packets_received = 0;
        let mut total_packets_lost: i64 = 0;
        let mut total_frames_decoded = 0;
        let mut total_frames_dropped = 0;
        let mut total_qp = 0;
        let mut total_jitter = 0.0;
        let mut total_freeze_count = 0;
        let mut total_freeze_duration = 0.0;
        let mut total_concealment = 0;
        let mut stream_count = 0;

        for report in stats.iter_by_type(RTCStatsType::InboundRTP) {
            if let RTCStatsReportEntry::InboundRtp(inbound) = report {
                // Метрики потерь
                total_packets_received += inbound.received_rtp_stream_stats.packets_received;
                total_packets_lost += inbound.received_rtp_stream_stats.packets_lost;
                
                // Метрики видео
                total_frames_decoded += inbound.frames_decoded;
                total_frames_dropped += inbound.frames_dropped;
                total_qp += inbound.qp_sum;
                total_jitter += inbound.received_rtp_stream_stats.jitter;
                total_freeze_count += inbound.freeze_count;
                total_freeze_duration += inbound.total_freezes_duration;
                total_concealment += inbound.concealment_events;
                
                stream_count += 1;
            }
        }

        // Сохраняем в current_stats
        self.current_stats.packets_received = total_packets_received;
        self.current_stats.packets_lost = total_packets_lost.max(0) as u64; // Отрицательные значения не учитываем
        
        self.current_stats.frames_decoded = total_frames_decoded;
        self.current_stats.frames_dropped = total_frames_dropped;
        
        // Средний QP
        if total_frames_decoded > 0 {
            self.current_stats.avg_qp = Some(total_qp as f64 / total_frames_decoded as f64);
        } else {
            self.current_stats.avg_qp = None;
        }
        
        // Средний джиттер
        if stream_count > 0 {
            self.current_stats.jitter = total_jitter / stream_count as f64;
        }
        
        self.current_stats.freeze_count = total_freeze_count;
        self.current_stats.total_freezes_duration = total_freeze_duration;
        self.current_stats.concealment_events = total_concealment;
    }

    // Расчет процента потерь за интервал
    fn calculate_loss_rate(&mut self) {
        let expected = self.current_stats.packets_received + self.current_stats.packets_lost;
        
        if expected > 0 {
            // Текущий процент потерь (кумулятивный с начала сессии)
            let cumulative_loss = self.current_stats.packets_lost as f64 / expected as f64 * 100.0;
            
            // Для "мгновенного" показателя нужно хранить предыдущие значения
            // Но пока используем кумулятивный
            self.current_stats.loss_rate = cumulative_loss;
            
            // Добавляем в историю (для трендов)
            self.current_stats.loss_history.push_back(cumulative_loss);
            if self.current_stats.loss_history.len() > 60 {
                self.current_stats.loss_history.pop_front();
            }
        }
    }

    // Определение качества
    pub fn get_quality(&self) -> Option<StreamQuality> {
        // 1. Проверяем потери - это самый важный показатель
        if self.current_stats.loss_rate > self.thresholds.high_loss {
            return Some(StreamQuality::Low);
        }
        
        if self.current_stats.loss_rate > self.thresholds.mid_loss {
            // При средних потерях смотрим, помогает ли FEC
            // Если качество все еще приемлемое, может быть Mid
            return Some(StreamQuality::Mid);
        }
        
        // 2. Если потери низкие, проверяем качество видео
        if self.current_stats.loss_rate <= self.thresholds.low_loss {
            // Смотрим на QP
            if let Some(avg_qp) = self.current_stats.avg_qp {
                if avg_qp < self.thresholds.high_qp_threshold {
                    return Some(StreamQuality::High);
                } else if avg_qp < self.thresholds.mid_qp_threshold {
                    return Some(StreamQuality::Mid);
                } else {
                    // Высокий QP = плохое качество даже при хорошей сети
                    return Some(StreamQuality::Low);
                }
            }
            
            // Проверяем дропы кадров
            let drop_ratio = if self.current_stats.frames_decoded > 0 {
                self.current_stats.frames_dropped as f64 / self.current_stats.frames_decoded as f64 * 100.0
            } else {
                0.0
            };
            
            if drop_ratio > self.thresholds.high_drop_ratio {
                return Some(StreamQuality::Low);
            }
            
            // Проверяем фризы
            if self.current_stats.total_freezes_duration > self.thresholds.high_freeze_duration {
                return Some(StreamQuality::Low);
            }
            
            // Все хорошо, но если QP неизвестен, возвращаем Mid
            return Some(StreamQuality::Mid);
        }
        
        // Если потери между mid и high, но качество видео хорошее
        if self.current_stats.loss_rate <= self.thresholds.high_loss {
            if let Some(avg_qp) = self.current_stats.avg_qp {
                if avg_qp < self.thresholds.high_qp_threshold {
                    return Some(StreamQuality::Mid); // Все еще может быть Mid
                }
            }
        }
        
        None
    }
    // Обновленный метод сохранения
    async fn save_stats(&mut self) -> Result<(), Error> {
        // Вычисляем качество и тренд перед сохранением
        let item = crate::StorageItem {
            connection_id: self.id.clone(),
            stats: &self.current_stats,
            timestamp: Utc::now(),
        };
        
        self.storage.insert(item)
            .await
            .map_err(|e| Error::SystemError { message: e.to_string().into() })?;
        
        Ok(())
    }
}

// Структура с порогами
#[derive(Debug, Clone)]
pub struct QualityThresholds {
    // Потери пакетов (в процентах)
    pub high_loss: f64,      // > 5% - плохо
    pub mid_loss: f64,       // > 2% - средне
    pub low_loss: f64,       // <= 1% - отлично
    
    // Метрики качества
    pub high_qp_threshold: f64,  // < 20 - высокое качество
    pub mid_qp_threshold: f64,   // < 30 - среднее качество
    
    pub high_freeze_duration: f64, // > 0.5 сек - плохо
    pub high_drop_ratio: f64,      // > 5% дропов - плохо
}

impl Default for QualityThresholds {
    fn default() -> Self {
        Self {
            high_loss: 5.0,
            mid_loss: 2.0,
            low_loss: 1.0,
            high_qp_threshold: 20.0,
            mid_qp_threshold: 30.0,
            high_freeze_duration: 0.5,
            high_drop_ratio: 5.0,
        }
    }
}

pub enum Trend {
    Improving, 
    Stable,     
    Worsening,  
    Unknown,    
}

