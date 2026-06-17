use std::{collections::HashSet, marker::PhantomData, sync::{Arc, atomic::AtomicUsize}};
use webrtc::{rtp::packet::Packet, track::{track_remote::TrackRemote}};
use crate::{actor::{Actor, Addr}, room::StreamQuality,};

pub struct RtpPacketForwarder<A: Actor> {
    pub subscriptions: HashSet<Addr<A>>,
    pub subscription_counter: Arc<AtomicUsize>,
    pub stream_quality: StreamQuality
}

impl<A: Actor> RtpPacketForwarder<A> where A::Message: From<(StreamQuality, Packet)>  {
    pub fn spawn(track: Arc<TrackRemote>, stream_quality: StreamQuality) -> Addr<Self> {
        let counter = Arc::new(AtomicUsize::new(0));
        let this: Addr<RtpPacketForwarder<A>> = Self {subscriptions: HashSet::new(), stream_quality,  subscription_counter: counter.clone()}
            .start_with_capacity(2048);
        let receiver = this.clone();
        tokio::spawn(async move {
            loop {
                let Ok((packet, _)) = track.read_rtp().await
                    .map_err(|e| tracing::error!("[RtpPacketForwarder] read_rtp {e}")) else { 
                        receiver.terminate().await;
                        break;
                    };
                if counter.load(std::sync::atomic::Ordering::Relaxed) == 0 { continue; };
                let Ok(_) = receiver.do_send(RtpPacketForwarderMessage::RtpPacket(packet))
                    .map_err(|_| tracing::error!("[RtpPacketForwarder] send RtpPacketForwarderMessage::RtpPacket(packet)")) 
                else { break; };
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
                self.subscription_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            },
            RtpPacketForwarderMessage::RtpPacket(packet) => {
                self.subscriptions.iter().for_each(|sub|{ let _ = sub.do_send((self.stream_quality, packet.clone()).into()); });
            },
            RtpPacketForwarderMessage::Unsubscribe(sub) => {
                self.subscriptions.remove(&sub);
                self.subscription_counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
    async fn stopping(self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[RtpPacketForwarder] Stopping");
    }
}

