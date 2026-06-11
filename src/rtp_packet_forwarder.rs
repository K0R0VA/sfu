use std::{collections::HashSet, marker::PhantomData, sync::Arc};
use webrtc::{rtp::packet::Packet, track::{track_remote::TrackRemote}};
use crate::{actor::{Actor, Addr}, audio_packet_forwarder::AudioPacketForwarder, room::StreamQuality, video_packet_forwarder::VideoPacketForwarder};

pub struct RtpPacketForwarder<A: Actor> {
    pub subscriptions: HashSet<Addr<A>>,
    pub stream_quality: StreamQuality
}

impl RtpPacketForwarder<AudioPacketForwarder> {
    pub fn spawn(track: Arc<TrackRemote>) -> Addr<Self> {
        let this = Self {subscriptions: HashSet::new(), stream_quality: StreamQuality::Audio}
            .start_with_capacity(32);
        let receiver = this.clone();
        tokio::spawn(async move {
            loop {
                let Ok((packet, _)) = track.read_rtp().await
                    .map_err(|e| tracing::error!("[RtpPacketForwarder] read_rtp {e}")) else { 
                        receiver.terminate().await;
                        break;
                    };
                let Ok(_) = receiver.do_send(RtpPacketForwarderMessage::RtpPacket(packet))
                    .map_err(|_| tracing::error!("[RtpPacketForwarder] send tpPacketForwarderMessage::RtpPacket(packet)")) 
                else { break; };
            }
        });
        this
    }
}

impl RtpPacketForwarder<VideoPacketForwarder> {
    pub fn spawn(track: Arc<TrackRemote>, stream_quality: StreamQuality) -> Addr<Self> {
        let this = Self {subscriptions: HashSet::new(), stream_quality}
            .start_with_capacity(32);
        let receiver = this.clone();
        tokio::spawn(async move {
            loop {
                let Ok((packet, _)) = track.read_rtp().await
                    .map_err(|e| tracing::error!("[RtpPacketForwarder] read_rtp {e}")) else { break; };
                let Ok(_) = receiver.do_send(RtpPacketForwarderMessage::RtpPacket(packet))
                    .map_err(|_| tracing::error!("[RtpPacketForwarder] send tpPacketForwarderMessage::RtpPacket(packet)")) else { break; };
            }
        });
        this
    }
}

pub enum RtpPacketForwarderMessage<A: Actor> {
    RtpPacket (Packet),
    Subscribe (Addr<A>),
    Unsubscribe (Addr<A>)
}


impl<A: Actor> Actor for RtpPacketForwarder<A> 
    where A::Message: From<(StreamQuality, Packet)> {
    type Message = RtpPacketForwarderMessage<A>;
    async fn starting(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[RtpPacketForwarder] Starting");
    }
    async fn handle(&mut self, _ctx: &mut crate::actor::Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            RtpPacketForwarderMessage::Subscribe(sub) => { 
                tracing::info!("[RtpPacketForwarder] Subscribe {:?}", PhantomData::<A>::default());
                self.subscriptions.insert(sub);
            },
            RtpPacketForwarderMessage::RtpPacket(packet) => {
                self.subscriptions.iter().for_each(|sub|{ let _ = sub.do_send((self.stream_quality, packet.clone()).into()); });
            },
            RtpPacketForwarderMessage::Unsubscribe(sub) => {
                self.subscriptions.remove(&sub);
            }
        }
    }
    async fn stopping(self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[RtpPacketForwarder] Stopping");
    }
}

