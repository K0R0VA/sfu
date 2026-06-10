use std::sync::Arc;

use webrtc::{rtp::packet::Packet, track::track_local::{TrackLocalWriter, track_local_static_rtp::TrackLocalStaticRTP}};

use crate::{actor::{Actor}, error::Error};

pub struct AudioPacketForwarder {
    pub track: Arc<TrackLocalStaticRTP>,
}

impl AudioPacketForwarder {
    async fn forward(&self, r: Packet) -> Result<(), Error> {
        self.track.write_rtp(&r).await?;
        Ok(())
    }
}

impl Actor for AudioPacketForwarder {
    type Message = Packet;
    async fn starting(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[PacketForwarder] Starting");
    }
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, packet: Self::Message) {
        if let Err(e) = self.forward(packet).await {
            tracing::error!("[PacketForwarder] write_rtp {e}");
            self.stop(ctx).await;
        }
    }
    async fn stopping(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[PacketForwarder] Stopping");
    }
}

