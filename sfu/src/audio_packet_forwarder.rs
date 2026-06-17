use std::sync::Arc;

use webrtc::{rtp::packet::Packet, track::track_local::{TrackLocalWriter, track_local_static_rtp::TrackLocalStaticRTP}};

use crate::{actor::Actor, error::Error, room::StreamQuality};

pub struct AudioPacketForwarder {
    pub track: Arc<TrackLocalStaticRTP>,
}

impl AudioPacketForwarder {
    async fn forward(&self, r: Packet) -> Result<(), Error> {
        self.track.write_rtp(&r).await?;
        Ok(())
    }
}

pub struct AudioPacketForwarderMessage {
    packet: Packet
}

impl From<(StreamQuality, Packet)> for AudioPacketForwarderMessage {
    fn from((_, packet): (StreamQuality, Packet)) -> Self {
        Self {packet}
    }
}

impl Actor for AudioPacketForwarder {
    type Message = AudioPacketForwarderMessage;
    async fn starting(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[AudioPacketForwarder] Starting");
    }
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, packet: Self::Message) {
        if let Err(e) = self.forward(packet.packet).await {
            tracing::error!("[AudioPacketForwarder] write_rtp {e}");
            self.stop(ctx).await;
        }
    }
    async fn stopping(self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[AudioPacketForwarder] Stopping");
    }
}

