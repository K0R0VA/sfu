use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::{SignalingClient, Storage, actor::{Actor, Addr, Ctx, StoppingExt, WeakAddr}, audio_packet_forwarder::AudioPacketForwarder, error::Error, keyframe_interceptor::KeyframeInterceptor, publisher::{Publisher, PublisherMessage}, room::{Codec, Room, RoomMessage, StreamQuality}, rtp_packet_gateway_router::{AudioRouterContext, RouterContext, RouterWaker, RtpPacketGatewayRouter, RtpPacketMessage, VideoRouterContext}, server::Key, subscriber::{Subscriber, SubscriberMessage}, video_packet_forwarder::VideoPacketForwarder};


pub struct User<K: Key, C: SignalingClient<UserKey = K>, S: Storage> {
    pub id: K,
    pub room: Addr<Room<K, C, S>>,
    pub signaling_client: C,
    pub publisher: WeakAddr<Publisher<K, C, S>>,
    pub subscriber: WeakAddr<Subscriber<K, C, S>>,
}

impl<K: Key, C: SignalingClient<UserKey = K>, S: Storage> User<K, C, S> {
    pub async fn new(user_id: K, signaling_client: C, room: Addr<Room<K, C, S>>) -> Result<Self, Error> {
        Ok(Self {
            id: user_id,
            room,
            signaling_client,
            publisher: WeakAddr::default(),
            subscriber: WeakAddr::default(),
        })
    }
}
#[derive(Deserialize)]
pub struct SessionParams {
    pub user_id: Option<Uuid>,
}

pub enum UserMessage<K: Key, C: SignalingClient> {
    Reconnect(C),
    SignalMessage(SignalMessage<K>),
    SwitchQualityLayer { quality: StreamQuality },
    SyncMessage(SyncMessage<K>),
    ConnectAudio(ConnectionRequest<K, AudioPacketForwarder, AudioRouterContext>),
    ConnectVideo { 
        request: ConnectionRequest<K, VideoPacketForwarder, VideoRouterContext>, 
        quality: StreamQuality , 
        keyframe_interceptor: Addr<KeyframeInterceptor>, 
        wake_notification: RouterWaker
    },
    Unsubscribe {
        user_id: K,
    },
    RoomClosed
}

pub enum SyncMessage<K> {
    Message(SignalMessage<K>),
    Close,
    Error(String)
}

pub struct ConnectionRequest<K: Key, T: Actor, R: RouterContext<T>> where T::Message: From<RtpPacketMessage>  {
    pub peer_id: K,
    pub gateway_router: Addr<RtpPacketGatewayRouter<T, R>>,
    pub codec_mime_type: Codec,
}

#[derive(Clone, Copy, Debug)]
pub enum ConnectionRequestKind {
    Audio,
    Video { stream_quality: StreamQuality }
}

impl<K: Key, C: SignalingClient<UserKey = K>, S: Storage> Actor for User<K, C, S> {
    type Message = UserMessage<K, C>;
    async fn handle(&mut self, ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            UserMessage::Reconnect(c) => {
                self.signaling_client = c;
            }
            UserMessage::SignalMessage(message) => {
                let _ = self.send_ws_message(message).await;
            }
            UserMessage::SwitchQualityLayer { quality } => {
                self.subscriber.try_send(SubscriberMessage::SwitchQualityLayer { quality }).await.ok_or_terminate(ctx);
                self.send_ws_message(SignalMessage::ConnectionQuality { quality }).await.ok_or_terminate(ctx);
            }
            UserMessage::SyncMessage(message) => {
                let _ = self.handle_ws_message(ctx, message).await;
            },
            UserMessage::ConnectAudio(request) => {
                self.subscriber.try_send(SubscriberMessage::ConnectAudio(request))
                    .await
                    .ok_or_terminate(ctx);
            },
            UserMessage::ConnectVideo {quality, request, keyframe_interceptor, wake_notification} => {
                self.subscriber.try_send(SubscriberMessage::ConnectVideo {quality, request, keyframe_interceptor, wake_notification})
                    .await
                    .ok_or_terminate(ctx);
            },
            UserMessage::RoomClosed => self.stop(ctx).await,
            UserMessage::Unsubscribe { user_id: speaker_id } => {
                self.unsubscribe(speaker_id).await.ok_or_terminate(ctx);
            }
        }
    }   
    async fn starting(&mut self, ctx: &Ctx<'_, Self>) {
        if let Err(e) = self.initiate(ctx).await {
            tracing::error!("[User] send_welcome {e}");
            return;
        }
    }
    async fn stopping(self, _: &Ctx<'_, Self>) {
        self.subscriber.try_terminate().await.ok();
        self.publisher.try_terminate().await.ok();
        let _ = self.room.send(RoomMessage::Leave { peer_id: self.id.clone() }).await;
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "kind", rename_all = "snake_case")] 
pub enum SignalMessage<K> {
    Rtc {
        target: Target,
        #[serde(flatten)] 
        message_type: MessageType,
    },
    ConnectionQuality { quality: StreamQuality },
    PeerLeft { peer_id: K },
}


#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")] 
pub enum MessageType {
    Offer { sdp: String },
    Answer { sdp: String },
    Candidate { 
        #[serde(flatten)]
        candidate: IceCandidate 
    },
    IceRestart { sdp: String }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct IceCandidate {
    pub candidate: String, 
    pub sdp_mid: Option<String>, 
    pub sdp_mline_index: Option<u16>,
}


#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum Target {
    Publisher = 0,
    Subscriber = 1
}

impl<K: Key, C: SignalingClient<UserKey = K>, S: Storage> User<K, C, S> {
    async fn initiate(&mut self, ctx: &Ctx<'_, Self>) -> Result<(), Error> {
        self.room.send(RoomMessage::Join { peer_id: self.id, addr: ctx.addr.clone() }).await?;
        let addr = ctx.addr.clone();
        let subscriber = Subscriber::new(addr.clone()).await?.start();
        self.subscriber.set_addr(subscriber);
        let publisher = Publisher::new(addr.clone(),  self.room.clone(), self.id).await?.start();
        self.publisher.set_addr(publisher);
        Ok(())
    }
    async fn handle_ws_message(&mut self, ctx: &mut Ctx<'_, Self>, message: SyncMessage<K>) -> Result<(), Error> {
        let message = match message {
            SyncMessage::Close => {
                self.stop(ctx).await;
                return Ok(());
            },
            SyncMessage::Message(text) => text,
            SyncMessage::Error(e) => return Err(Error::SystemError { message: e.into() })
        };
        match message {
            SignalMessage::Rtc { target, message_type } => {
                match target {
                    Target::Publisher => {
                        let _ = self.publisher.try_send(PublisherMessage::Websocket(message_type)).await;
                    }
                    Target::Subscriber => {
                        let _ = self.subscriber.try_send(SubscriberMessage::Websocket(message_type)).await;
                    }
                }
            },
            _ => {}
        }
        Ok(())
    }
    async fn send_ws_message(&mut self, msg: SignalMessage<K>) -> Result<(), Error> {
        self.signaling_client.send(msg.into())
            .await
            .map_err(|e| Error::SystemError { message: e.to_string().into() })?;
        Ok(())
    }
    async fn unsubscribe(&mut self, peer_id: K) -> Result<(), Error> {
        let _ = self.subscriber.try_send(SubscriberMessage::Unsubscribe { peer_id }).await;
        self.signaling_client.send(SignalMessage::PeerLeft { peer_id }.into())
            .await
            .map_err(|e| Error::SystemError { message: e.to_string().into() })?;
        Ok(())
    }
}