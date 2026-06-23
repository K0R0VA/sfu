use std::{collections::HashSet, marker::PhantomData, sync::{Arc}};
use webrtc::{rtp::packet::Packet, track::{track_remote::TrackRemote}};
use crate::{actor::{Actor, Addr}, pli_sender::{Ping, PliSender}, room::StreamQuality,};

pub struct RtpPacketGatewayRouter<A: Actor> {
    pub subscriptions: HashSet<Addr<A>>,
    pub stream_quality: StreamQuality,
    pub pli_sender: Option<Addr<PliSender>,>
}

impl<A: Actor> RtpPacketGatewayRouter<A> where A::Message: From<(StreamQuality, Packet)>  {
    pub fn spawn(track: Arc<TrackRemote>, stream_quality: StreamQuality, pli_sender: Option<Addr<PliSender>>) -> Addr<Self> {
        let this: Addr<RtpPacketGatewayRouter<A>> = Self {subscriptions: HashSet::new(), stream_quality, pli_sender}
            .start_with_capacity(2048);
        let receiver = this.clone();
        tokio::spawn(async move {
            loop {
                let Ok((packet, _)) = track.read_rtp().await
                    .map_err(|e| tracing::error!("[RtpPacketGatewayRouter] read_rtp {e}")) else { 
                        receiver.terminate().await;
                        break;
                    };
                let Ok(_) = receiver.do_send(RtpPacketGatewayRouterMessage::RtpPacket(packet))
                    .map_err(|_| tracing::error!("[RtpPacketGatewayRouter] send RtpPacketForwarderMessage::RtpPacket(packet)")) 
                else { break; };
            }
        });
        this
    }
}

pub enum RtpPacketGatewayRouterMessage<A: Actor> {
    RtpPacket (Packet),
    Subscribe (Addr<A>),
    Unsubscribe (Addr<A>),
    ForcePli
}


impl<A: Actor> Actor for RtpPacketGatewayRouter<A> 
    where A::Message: From<(StreamQuality, Packet)> {
    type Message = RtpPacketGatewayRouterMessage<A>;
    async fn starting(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[RtpPacketForwarder] Starting");
    }
    async fn handle(&mut self, _ctx: &mut crate::actor::Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            RtpPacketGatewayRouterMessage::ForcePli => if let Some(pli_sender) = &self.pli_sender {
                let _ = pli_sender.send(Ping).await;
            }
            RtpPacketGatewayRouterMessage::Subscribe(sub) => { 
                tracing::info!("[RtpPacketForwarder] Subscribe {:?}", PhantomData::<A>::default());
                self.subscriptions.insert(sub);
                if let Some(pli_sender) = &self.pli_sender {
                    let _ = pli_sender.send(Ping).await;
                }
            },
            RtpPacketGatewayRouterMessage::RtpPacket(packet) => {
                self.subscriptions.iter().for_each(|sub|{ let _ = sub.do_send((self.stream_quality, packet.clone()).into()); });
            },
            RtpPacketGatewayRouterMessage::Unsubscribe(sub) => {
                self.subscriptions.remove(&sub);
            }
        }
    }
    async fn stopping(self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[RtpPacketForwarder] Stopping");
    }
}

