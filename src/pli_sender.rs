use std::{sync::Arc, time::{Duration, Instant}};

use webrtc::{peer_connection::RTCPeerConnection, rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication};

use crate::actor::Actor;

pub struct PliSender {
    pc: Arc<RTCPeerConnection>,
    ssrc: u32,
    instant: Instant,
}

impl PliSender {
    pub fn new(pc: Arc<RTCPeerConnection>, ssrc: u32) -> Self {
        let instant = Instant::now() - Duration::from_secs(6);
        Self {
            instant,
            pc,
            ssrc
        }
    }
}

pub struct Ping;

impl Actor for PliSender {
    type Message = Ping;
    async fn starting(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[PliSender] starting");
    }
    async fn stopping(self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[PliSender] stopping");
    }
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, _: Self::Message) {
        let elapsed = self.instant.elapsed();
        if elapsed < Duration::from_secs(5) { 
            return;
        }
        tracing::info!("[PliSender] Send");
        if let Err(e) = self.pc.write_rtcp(&[Box::new(PictureLossIndication {
            media_ssrc: self.ssrc,
            sender_ssrc: 0
        })]).await {
            tracing::error!("[PliSender] write_rtcp {e}");
            self.stop(ctx).await;
        }
        self.instant = Instant::now();
    }
}