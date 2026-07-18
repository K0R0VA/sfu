use serde::{Deserialize, Serialize};
use uuid::Uuid;
use webrtc::{peer_connection::{RTCPeerConnection, offer_answer_options::RTCOfferOptions, }, rtp::packet::Packet, };
use crate::{Storage, SyncChannel, actor::{Actor, Addr, Ctx, WeakAddr}, audio_packet_forwarder::AudioPacketForwarder, error::Error, keyframe_interceptor::KeyframeInterceptor, publisher_pc::{Publisher, PublisherMessage}, quality_monitor::DeviceType, room::{MimeType, Room, RoomMessage, StreamQuality}, rtp_packet_gateway_router::RtpPacketGatewayRouter, subscriber_pc::{Subscriber, SubscriberMessage}, video_packet_forwarder::VideoPacketForwarder};

pub struct User<C: SyncChannel, S: Storage> {
    pub room: Addr<Room<C, S>>,
    pub peer_id: Uuid,
    // pub sync_channel: 
    pub sync_channel: C, // 
    pub publisher: WeakAddr<Publisher<C, S>>,
    pub subscriber: WeakAddr<Subscriber<C, S>>,
}

impl<C: SyncChannel, S: Storage> User<C, S> {
    pub async fn new(sync_channel: C, room: Addr<Room<C, S>>) -> Result<Self, Error> {
        let peer_id = Uuid::new_v4();
        Ok(Self {
            peer_id,
            room,
            sync_channel,
            publisher: WeakAddr::default(),
            subscriber: WeakAddr::default(),
        })
    }
}

pub enum UserMessage {
    SignalMessage(SignalMessage),
    SwitchQualityLayer { quality: StreamQuality },
    SyncMessage(SyncMessage),
    ConnectAudio(ConnectionRequest<AudioPacketForwarder>),
    ConnectVideo { request: ConnectionRequest<VideoPacketForwarder>, quality: StreamQuality , keyframe_interceptor: Addr<KeyframeInterceptor>},
    Unsubscribe {
        user_id: Uuid,
    },
    RoomClosed
}

pub enum SyncMessage {
    Message(SignalMessage),
    Close,
    Error(String)
}

pub struct ConnectionRequest<T: Actor> where T::Message: From<(StreamQuality, Packet)>  {
    pub peer_id: Uuid,
    pub gateway_router: Addr<RtpPacketGatewayRouter<T>>,
    pub codec_mime_type: MimeType,
}

#[derive(Clone, Copy, Debug)]
pub enum ConnectionRequestKind {
    Audio,
    Video { stream_quality: StreamQuality }
}

impl<C: SyncChannel, S: Storage> Actor for User<C, S> {
    type Message = UserMessage;
    async fn handle(&mut self, ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            UserMessage::SignalMessage(message) => {
                if let Err(e) = self.send_ws_message(message).await {
                    tracing::error!("👤 [UserActor] UserMessage::Websocket {:?}", e);
                    self.stop(ctx).await;
                }
            }
            UserMessage::SwitchQualityLayer { quality } => {
                let _ = self.subscriber.try_send(SubscriberMessage::SwitchQualityLayer { quality }).await;
            }
            UserMessage::SyncMessage(message) => {
                if let Err(e) = self.handle_ws_message(ctx, message).await {
                    tracing::error!("👤 [UserActor] UserMessage::Websocket {:?}", e);
                    self.stop(ctx).await;
                }
            },
            UserMessage::ConnectAudio(request) => {
                let _ = self.subscriber.try_send(SubscriberMessage::ConnectAudio(request)).await;
            },
            UserMessage::ConnectVideo {quality, request, keyframe_interceptor} => {
                let _ = self.subscriber.try_send(SubscriberMessage::ConnectVideo {quality, request, keyframe_interceptor}).await;
            },
            UserMessage::RoomClosed => self.stop(ctx).await,
            UserMessage::Unsubscribe { user_id: speaker_id } => {
                if let Err(e) = self.unsubscribe(speaker_id).await {
                    tracing::error!("👤 [UserActor] UserMessage::DisconnectFromUser {:?}", e);
                    self.stop(ctx).await;
                }
            }
        }
    }   
    async fn starting(&mut self, ctx: &Ctx<'_, Self>) {
        if let Err(e) = self.initiate(ctx).await {
            tracing::error!("[User] send_welcome {e}");
            return;
        }
        tracing::info!("🟢 [Userctor] Пользователь инициализирован.");
    }
    async fn stopping(self, _: &Ctx<'_, Self>) {
        self.subscriber.try_terminate().await.ok();
        self.publisher.try_terminate().await.ok();
        let _ = self.room.send(RoomMessage::Leave { peer_id: self.peer_id.clone() }).await;
        tracing::info!("🔴 [UserActor] Пользователь уничтожен.");
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "kind", rename_all = "snake_case")] 
pub enum SignalMessage {
    Rtc {
        target: Target,
        #[serde(flatten)] 
        message_type: MessageType,
    },
    RoomInfo { name: String, },
    Welcome { peer_id: Uuid, },
    PeerLeft { peer_id: Uuid },
    Connect { device_type: DeviceType },
}

impl From<SignalMessage> for String {
    fn from(value: SignalMessage) -> Self {
        let str = serde_json::json!(value);
        str.to_string()
    }
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
pub enum Target {
    Publisher,
    Subscriber
}

impl<C: SyncChannel, S: Storage> User<C, S> {
    async fn initiate(&mut self, ctx: &Ctx<'_, Self>) -> Result<(), Error> {
        let addr = ctx.addr.clone();
        let subscriber = Subscriber::new(addr.clone()).await?.start();
        self.subscriber.set_addr(subscriber);
        let publisher = Publisher::new(addr.clone(), self.room.clone(), self.peer_id).await?.start();
        self.publisher.set_addr(publisher);
        self.send_welcome().await?;
        Ok(())
    }
    async fn send_welcome(&mut self) -> Result<(), Error> {
        self.sync_channel.send(SignalMessage::Welcome { peer_id: self.peer_id }.into())
            .await
            .map_err(|e| Error::SystemError { message: e.to_string().into() })?;
        Ok(())
    }
    async fn handle_ws_message(&mut self, ctx: &mut Ctx<'_, Self>, message: SyncMessage) -> Result<(), Error> {
        let message = match message {
            SyncMessage::Close => {
                self.stop(ctx).await;
                return Ok(());
            },
            SyncMessage::Message(text) => text,
            SyncMessage::Error(e) => return Err(Error::SystemError { message: e.into() })
        };
        match message {
            SignalMessage::Connect { device_type } => {
                let _ = self.publisher.try_send(PublisherMessage::InitiateMonitoring { device_type }).await;
            },
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
    async fn send_ws_message(&mut self, msg: SignalMessage) -> Result<(), Error> {
        self.sync_channel.send(msg.into())
            .await
            .map_err(|e| Error::SystemError { message: e.to_string().into() })?;
        Ok(())
    }
    async fn unsubscribe(&mut self, peer_id: Uuid) -> Result<(), Error> {
        let _ = self.subscriber.try_send(SubscriberMessage::Unsubscribe { peer_id }).await;
        self.sync_channel.send(SignalMessage::PeerLeft { peer_id }.into())
            .await
            .map_err(|e| Error::SystemError { message: e.to_string().into() })?;
        Ok(())
    }
}

pub async fn initiate_ice_restart(
    pc: &RTCPeerConnection,
    target: Target,
) -> Result<SignalMessage, Error> {
    let mut options = RTCOfferOptions::default();
    options.ice_restart = true;
    let offer = pc.create_offer(Some(options)).await?;
    pc.set_local_description(offer.clone()).await?;
    let message = SignalMessage::Rtc {
        target, 
        message_type: MessageType::IceRestart { sdp: offer.sdp } 
    };
    Ok(message)
}