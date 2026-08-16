use std::{collections::HashSet, sync::Arc, time::{Duration, Instant}};
use rtc::{rtcp::payload_feedbacks::full_intra_request::{FirEntry, FullIntraRequest}, rtp::Packet};
use webrtc::media_stream::track_remote::{TrackRemote, TrackRemoteEvent};

use crate::{actor::{Actor, Addr, StoppingExt}, audio_packet_forwarder::AudioPacketForwarder, error::Error, room::StreamQuality, video_packet_forwarder::VideoPacketForwarder,};

pub struct RtpPacketGatewayRouter<A: Actor, T> {
    pub subscriptions: HashSet<Addr<A>>,
    pub context: T,
}
#[derive(Clone)]
pub struct VideoRouterContext {
    pub track: Arc<dyn TrackRemote>,
    pub stream_quality: StreamQuality,
    pub ssrc: u32,
    pub fir_sequence: u8,
    pub is_sleeping: bool,
    pub wake_notifier: tokio::sync::broadcast::Sender<()>,
}

impl VideoRouterContext {
    pub fn new(track: Arc<dyn TrackRemote>, stream_quality: StreamQuality, ssrc: u32, wake_notifier: tokio::sync::broadcast::Sender<()> ) -> Self {
        Self {
            track,
            fir_sequence: 0,
            is_sleeping: false,
            ssrc,
            stream_quality,
            wake_notifier,
        }
    }
}

impl<A: Actor> RouterContext<A> for VideoRouterContext where A::Message: From<RtpPacketMessage> {
    fn send_fir(&mut self) {
        let ssrc = self.ssrc;
        let sequence_number = self.fir_sequence;
        let track = self.track.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            track.write_rtcp(vec![Box::new(FullIntraRequest {
                media_ssrc: ssrc,
                sender_ssrc: 0,
                fir: [
                    FirEntry { ssrc: ssrc, sequence_number }
                ].to_vec()
            })]).await?;
            Result::<(), Error>::Ok(())
        });
        self.fir_sequence = self.fir_sequence.wrapping_add(1);
    }
    fn try_awake(&mut self) {
        if self.is_sleeping {
            let _ = self.wake_notifier.send(());
            self.is_sleeping = false;
        }
    }
    fn set_sleeping(&mut self) {
        self.is_sleeping = true;
    }
    fn stream_quality(&self) -> StreamQuality {
        self.stream_quality
    }
}
#[derive(Clone, Copy)]
pub struct AudioRouterContext {}

impl<A: Actor> RouterContext<A> for AudioRouterContext where A::Message: From<RtpPacketMessage> {
    fn send_fir(&mut self) {}
    fn try_awake(&mut self) {}
    fn set_sleeping(&mut self) {}
    fn stream_quality(&self) -> StreamQuality {
        StreamQuality::Audio
    }
}


impl<A: Actor, R: RouterContext<A>> RtpPacketGatewayRouter<A, R> where A::Message: From<RtpPacketMessage>  {
    pub fn new(context: R) -> Self {
        Self {
            subscriptions: HashSet::new(), 
            context,
        }
    }
    pub fn spawn(
            track: Arc<dyn TrackRemote>, 
            context: R,
        ) -> Addr<Self> {
        let this: Addr<RtpPacketGatewayRouter<A, R>> = Self {
            subscriptions: HashSet::new(), 
            context,
        }
            .start_with_capacity(32);
        let receiver = this.clone();
        tokio::spawn(async move {
            loop {
                let message = match track.poll().await {
                    Some(TrackRemoteEvent::OnRtpPacket(mut packet)) => {
                        packet.header.extensions.clear();
                        RtpPacketGatewayRouterMessage::RtpPacket(packet)
                    },
                    Some(TrackRemoteEvent::OnEnded) | Some(TrackRemoteEvent::OnEnding) => break,
                    Some(TrackRemoteEvent::OnError) => {
                        break;
                    }
                    _ => continue,
                };
                let Ok(_) = receiver.do_send(message) else { break; };
            }
        });
        this
    }
}

pub trait RouterContext<A: Actor>: Send  + Clone + 'static  {
    fn send_fir(&mut self);
    fn try_awake(&mut self);
    fn set_sleeping(&mut self);
    fn stream_quality(&self) -> StreamQuality;
}

pub enum RtpPacketGatewayRouterMessage<A: Actor> {
    RtpPacket (Packet),
    Timeout,
    Subscribe (Addr<A>),
    Unsubscribe (Addr<A>),
}


pub enum RtpPacketMessage {
    Packet(StreamQuality, Packet),
    Timeout
}


impl<A: Actor, R: RouterContext<A>> Actor for RtpPacketGatewayRouter<A, R> 
    where A::Message: From<RtpPacketMessage> {
    type Message = RtpPacketGatewayRouterMessage<A>;
    async fn starting(&mut self, _ctx: &crate::actor::Ctx<'_, Self>) {}
    async fn handle(&mut self, ctx: &mut crate::actor::Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            RtpPacketGatewayRouterMessage::Subscribe(sub) => { 
                let is_new = self.subscriptions.insert(sub);
                if !is_new {
                    tracing::warn!("Sub was already insert at quality {:?}", self.context.stream_quality());
                }
                self.context.send_fir();
            },
            RtpPacketGatewayRouterMessage::Unsubscribe(sub) => {
                self.subscriptions.remove(&sub);
            }
            RtpPacketGatewayRouterMessage::RtpPacket(packet) => {
                for sub in &self.subscriptions {
                    let message = RtpPacketMessage::Packet(self.context.stream_quality(), packet.clone());
                    sub.do_send(message.into()).ok_or_terminate(ctx);
                }
                self.context.try_awake();
            },
            RtpPacketGatewayRouterMessage::Timeout => {
                self.context.set_sleeping();
                for sub in &self.subscriptions {
                    let message = RtpPacketMessage::Timeout;
                    sub.do_send(message.into()).ok_or_terminate(ctx);
                }
                self.subscriptions.clear();
            }
        }
    }
    async fn stopping(self, _ctx: &crate::actor::Ctx<'_, Self>) {}
}

pub type VideoRouter = Addr<RtpPacketGatewayRouter<VideoPacketForwarder, VideoRouterContext>>;
pub type AudioRouter = Addr<RtpPacketGatewayRouter<AudioPacketForwarder, AudioRouterContext>>;
pub type RouterWaker = Arc<tokio::sync::broadcast::Receiver<()>>;