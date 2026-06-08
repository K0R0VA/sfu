use std::sync::Arc;

use futures_util::future::Either;
use webrtc::{rtp::packet::Packet, track::track_local::{TrackLocalWriter, track_local_static_rtp::TrackLocalStaticRTP}};

use crate::{actor::{Actor, Addr}, audio_subscription::AudioSubscription, error::Error, video_subscription::VideoSubscription};

pub struct PacketForwarder {
    pub track: Arc<TrackLocalStaticRTP>,
    pub owner: Either<Addr<AudioSubscription>, Addr<VideoSubscription>>
}

impl PacketForwarder {
    async fn forward(&self, r: Packet) -> Result<(), Error> {
        self.track.write_rtp(&r).await?;
        Ok(())
    }
}

impl Actor for PacketForwarder {
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

