use std::{sync::Arc, time::{Duration, Instant}};

use webrtc::{peer_connection::RTCPeerConnection, rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication};

use crate::actor::{Actor, StoppingExt};

pub struct KeyframeInterceptor {
    pc: Arc<RTCPeerConnection>,
    ssrc: u32,
    instant: Instant,
}

impl KeyframeInterceptor {
    pub fn new(pc: Arc<RTCPeerConnection>, ssrc: u32) -> Self {
        let instant = Instant::now() - Duration::from_secs(6);
        Self {
            instant,
            pc,
            ssrc
        }
    }
    async fn send_pli(&mut self) -> Result<(), crate::Error> {
        let elapsed = self.instant.elapsed();
        if elapsed < Duration::from_secs(2) { 
            return Ok(());
        }
        self.pc.write_rtcp(&[Box::new(PictureLossIndication {
            media_ssrc: self.ssrc,
            sender_ssrc: 0
        })]).await?;
        self.instant = Instant::now();
        Ok(()) 
    }
}

pub struct RequestKeyframe;

impl Actor for KeyframeInterceptor {
    type Message = RequestKeyframe;
    async fn starting(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[PliSender] starting");
    }
    async fn stopping(self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[PliSender] stopping");
    }
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, _: Self::Message) {
        self.send_pli().await.ok_or_terminate(ctx);
    }
}