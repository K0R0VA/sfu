use std::{collections::HashSet, sync::Arc, time::{Duration, Instant}};
use webrtc::{peer_connection::RTCPeerConnection, rtcp::payload_feedbacks::{full_intra_request::{FirEntry, FullIntraRequest}}, rtp::packet::Packet, track::track_remote::TrackRemote};
use crate::{actor::{Actor, Addr, StoppingExt}, audio_packet_forwarder::AudioPacketForwarder, error::Error, room::StreamQuality, video_packet_forwarder::VideoPacketForwarder,};

pub struct RtpPacketGatewayRouter<A: Actor, T> {
    pub subscriptions: HashSet<Addr<A>>,
    pub period_packets: usize,
    pub period: Instant,
    pub context: T,
}
#[derive(Clone)]
pub struct VideoRouterContext {
    pub pc: Arc<RTCPeerConnection>,
    pub stream_quality: StreamQuality,
    pub ssrc: u32,
    pub fir_sequence: u8,
    pub is_sleeping: bool,
    pub wake_notifier: tokio::sync::broadcast::Sender<()>,
}

impl VideoRouterContext {
    pub fn new(pc: Arc<RTCPeerConnection>, stream_quality: StreamQuality, ssrc: u32, wake_notifier: tokio::sync::broadcast::Sender<()> ) -> Self {
        Self {
            fir_sequence: 0,
            is_sleeping: false,
            pc,
            ssrc,
            stream_quality,
            wake_notifier,
        }
    }
}

impl<A: Actor> RouterContext<A> for VideoRouterContext where A::Message: From<RtpPacketMessage> {
    fn send_fir(&mut self) {
        let pc = self.pc.clone();
        let ssrc = self.ssrc;
        let sequence_number = self.fir_sequence;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            pc.write_rtcp(&[Box::new(FullIntraRequest {
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
    pub fn spawn(
            track: Arc<TrackRemote>, 
            context: R,
        ) -> Addr<Self> {
        let this: Addr<RtpPacketGatewayRouter<A, R>> = Self {
            subscriptions: HashSet::new(), 
            context,
            period: Instant::now(), 
            period_packets: 0
        }
            .start_with_capacity(2048);
        let receiver = this.clone();
        tokio::spawn(async move {
            let timeout_period = Duration::from_millis(300);
            let mut is_timeout_send = false;
            loop {
                let read_rtp_fut = track.read_rtp();
                let fut = tokio::time::timeout(timeout_period, read_rtp_fut);
                let message = match fut.await {
                    Ok(Ok((packet, _))) => {
                        is_timeout_send = false;
                        RtpPacketGatewayRouterMessage::RtpPacket(packet)
                    },
                    Ok(Err(webrtc::Error::ErrClosedPipe)) | 
                    Ok(Err(webrtc::Error::Data(webrtc::data::Error::Util(webrtc::util::Error::ErrBufferClosed)))) => {
                        let _ = receiver.terminate().await;
                        break;
                    },
                    Ok(Err(e)) => {
                        tracing::error!("{e}");
                        let _ = receiver.terminate().await;
                        break;
                    },
                    Err(_) if is_timeout_send => continue,
                    Err(_) => {
                        is_timeout_send = true;
                        RtpPacketGatewayRouterMessage::Timeout
                    }
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