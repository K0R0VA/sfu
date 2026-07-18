use std::{collections::HashSet, marker::PhantomData, sync::Arc};
use webrtc::{peer_connection::RTCPeerConnection, rtcp::payload_feedbacks::full_intra_request::{FirEntry, FullIntraRequest}, rtp::packet::Packet, track::track_remote::TrackRemote};
use crate::{actor::{Actor, Addr, StoppingExt}, error::Error, room::StreamQuality,};

pub struct RtpPacketGatewayRouter<A: Actor> {
    pub subscriptions: HashSet<Addr<A>>,
    pub stream_quality: StreamQuality,
    pub pc: Arc<RTCPeerConnection>,
    pub ssrc: u32,
    pub fir_sequence: u8,
}

impl<A: Actor> RtpPacketGatewayRouter<A> where A::Message: From<(StreamQuality, Packet)>  {
    pub fn spawn(track: Arc<TrackRemote>, stream_quality: StreamQuality, ssrc: u32, pc: Arc<RTCPeerConnection>) -> Addr<Self> {
        let this: Addr<RtpPacketGatewayRouter<A>> = Self {subscriptions: HashSet::new(), ssrc, stream_quality, pc, fir_sequence: 0}
            .start_with_capacity(32);
        let receiver = this.clone();
        tokio::spawn(async move {
            loop {
                let Ok((packet, _)) = track.read_rtp().await
                    .map_err(|e| tracing::error!("[RtpPacketGatewayRouter] read_rtp {e}")) else { 
                        let _ = receiver.terminate().await;
                        break;
                    };
                let Ok(_) = receiver.do_send(RtpPacketGatewayRouterMessage::RtpPacket(packet))
                    .map_err(|_| tracing::error!("[RtpPacketGatewayRouter] send RtpPacketForwarderMessage::RtpPacket(packet)")) 
                else { break; };
            }
        });
        this
    }
    pub async fn add_subscriber(&mut self, sub: Addr<A>) -> Result<(), Error> {
        self.subscriptions.insert(sub);
        self.pc.write_rtcp(&[Box::new(FullIntraRequest {
            media_ssrc: self.ssrc,
            sender_ssrc: 0,
            fir: [
                FirEntry { ssrc: self.ssrc, sequence_number: self.fir_sequence }
            ].to_vec()
        })]).await?;
        self.fir_sequence = self.fir_sequence.wrapping_add(1);
        Ok(())
    } 
}

pub enum RtpPacketGatewayRouterMessage<A: Actor> {
    RtpPacket (Packet),
    Subscribe (Addr<A>),
    Unsubscribe (Addr<A>),
}


impl<A: Actor> Actor for RtpPacketGatewayRouter<A> 
    where A::Message: From<(StreamQuality, Packet)> {
    type Message = RtpPacketGatewayRouterMessage<A>;
    async fn starting(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[RtpPacketForwarder] Starting");
    }
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            RtpPacketGatewayRouterMessage::Subscribe(sub) => { 
                tracing::info!("[RtpPacketForwarder] Subscribe {:?}", PhantomData::<A>::default());
                self.add_subscriber(sub).await.ok_or_terminate(ctx);
            },
            RtpPacketGatewayRouterMessage::RtpPacket(packet) => {
                for sub in &self.subscriptions {
                    sub.do_send((self.stream_quality, packet.clone()).into()).ok_or_terminate(ctx);
                }
            },
            RtpPacketGatewayRouterMessage::Unsubscribe(sub) => {
                self.subscriptions.remove(&sub);
            }
        }
    }
    async fn stopping(self, _ctx: &crate::actor::Ctx<'_, Self>) {
        tracing::info!("[RtpPacketGatewayRouter] Stopping");
    }
}

