use std::{sync::Arc, time::{Duration, Instant}};

use webrtc::{peer_connection::RTCPeerConnection, rtcp::{packet::Packet, payload_feedbacks::{full_intra_request::{FirEntry, FullIntraRequest}, picture_loss_indication::PictureLossIndication}}};

use crate::actor::{Actor};

pub struct KeyframeInterceptor {
    pc: Arc<RTCPeerConnection>,
    ssrc: u32,
    sequanse_number: u8,
    instant: Instant,
}

impl KeyframeInterceptor {
    pub fn new(pc: Arc<RTCPeerConnection>, ssrc: u32) -> Self {
        let instant = Instant::now() - Duration::from_secs(6);
        Self {
            instant,
            pc,
            ssrc,
            sequanse_number: 0,
        }
    }
    async fn send_request(&mut self, request: RequestKeyframe) {
        let elapsed = self.instant.elapsed();
        let rtcp: [Box<dyn Packet + Send + Sync + 'static>; 1] = match request {
            RequestKeyframe::Fir => [Box::new(FullIntraRequest {
                media_ssrc: self.ssrc,
                sender_ssrc: 0,
                fir: [
                    FirEntry { ssrc: self.ssrc, sequence_number: self.sequanse_number }
                ].to_vec()
            })],
            RequestKeyframe::Pli if elapsed < Duration::from_secs(2) => [Box::new(PictureLossIndication {
                media_ssrc: self.ssrc,
                sender_ssrc: 0
            })],
            _ => { return; }
        };
        let _ = self.pc.write_rtcp(&rtcp).await;
        self.instant = Instant::now();
        self.sequanse_number = self.sequanse_number.wrapping_add(1);
    }
}

pub enum RequestKeyframe {
    Pli,
    Fir
}

impl Actor for KeyframeInterceptor {
    type Message = RequestKeyframe;
    async fn starting(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {}
    async fn stopping(self, _ctx: &crate::actor::Ctx<'_, Self>) {}
    async fn handle(&mut self, _ctx: &mut crate::actor::Ctx<'_, Self>, request: Self::Message) {
        self.send_request(request).await;
    }
}