use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, stream::{SplitSink}};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use webrtc::{ice_transport::{ice_connection_state::RTCIceConnectionState}, peer_connection::{RTCPeerConnection, offer_answer_options::RTCOfferOptions, }, };
use crate::{PacketAudioSubscription, PacketVideoSubscription, actor::{Actor, Addr, Ctx, WeakAddr},error::Error, pli_sender::PliSender, publisher_pc::{Publisher, PublisherMessage}, quality_monitor::{DeviceType, QualityMonitor, QualityThresholds}, room::{MimeType, Room, RoomMessage, StreamQuality}, subscriber_pc::{Subscriber, SubscriberMessage}};

pub struct User {
    pub room: Addr<Room>,
    pub peer_id: Uuid,
    pub ws_tx: SplitSink<WebSocket, Message>,
    pub publisher: WeakAddr<Publisher>,
    pub subscriber: WeakAddr<Subscriber>,
}

impl User {
    pub async fn new(ws_tx: SplitSink<WebSocket, Message>, room: Addr<Room>) -> Result<Self, Error> {
        let peer_id = Uuid::new_v4();
        Ok(Self {
            peer_id,
            room,
            ws_tx,
            publisher: WeakAddr::default(),
            subscriber: WeakAddr::default(),
        })
    }
}

pub enum UserMessage {
    SignalMessage(SignalMessage),
    SwitchQualityLayer { quality: StreamQuality },
    Websocket {
        message: Result<Message, Error>
    },
    ConnectAudio(ConnectionRequest<PacketAudioSubscription>),
    ConnectVideo { request: ConnectionRequest<PacketVideoSubscription>, quality: StreamQuality },
    Unsubscribe {
        user_id: Uuid,
    },
    RoomClosed
}

pub struct ConnectionRequest<T> {
    pub speaker_id: Uuid,
    pub stream: T,
    pub codec_mime_type: MimeType,
}

#[derive(Clone, Copy, Debug)]
pub enum ConnectionRequestKind {
    Audio,
    Video { stream_quality: StreamQuality }
}

impl Actor for User {
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
                let _ = self.subscriber.strong().send(SubscriberMessage::SwitchQualityLayer { quality }).await;
            }
            UserMessage::Websocket { message } => {
                if let Err(e) = self.handle_ws_message(ctx, message).await {
                    tracing::error!("👤 [UserActor] UserMessage::Websocket {:?}", e);
                    self.stop(ctx).await;
                }
            },
            UserMessage::ConnectAudio(request) => {
                let _ = self.subscriber.strong().send(SubscriberMessage::ConnectAudio(request)).await;
            },
            UserMessage::ConnectVideo {quality, request} => {
                let _ = self.subscriber.strong().send(SubscriberMessage::ConnectVideo {quality, request}).await;
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
        self.subscriber.strong().terminate().await;
        self.publisher.strong().terminate().await;
        let _ = self.room.send(RoomMessage::Leave { peer_id: self.peer_id.clone() }).await;
        tracing::info!("🔴 [UserActor] Пользователь уничтожен.");
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(untagged, rename_all = "snake_case")] 
pub enum SignalMessage {
    Rtc {
        target: Target,
        #[serde(flatten)] 
        message_type: MessageType,
    },
    DeviceType { device_type: DeviceType }
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

impl User {
    async fn initiate(&mut self, ctx: &Ctx<'_, Self>) -> Result<(), Error> {
        self.send_welcome().await?;
        let addr = ctx.addr.clone();
        let subscriber = Subscriber::new(addr.clone()).await?.start();
        self.subscriber.set_addr(subscriber);
        let publisher = Publisher::new(addr.clone(), self.room.clone(), self.peer_id).await?.start();
        self.publisher.set_addr(publisher);
        Ok(())
    }
    async fn send_welcome(&mut self) -> Result<(), Error> {
        let welcome_payload = serde_json::json!({
            "type": "welcome",
            "assigned_peer_id": self.peer_id
        });
        self.ws_tx.send(serde_json::to_string(&welcome_payload)?.into()).await?;
        Ok(())
    }
    async fn handle_ice_state_change(&mut self, ctx: &mut Ctx<'_, Self>, state: RTCIceConnectionState, target: Target) -> Result<(), Error> {
        match (state, target) {
            (RTCIceConnectionState::Failed, target) => {
                // self.initiate_ice_restart(target).await?;
            },
            (RTCIceConnectionState::Disconnected, Target::Publisher) => {
            },
            (RTCIceConnectionState::Disconnected, Target::Subscriber) => {},
            (RTCIceConnectionState::Connected, Target::Publisher) => {},
            (RTCIceConnectionState::Connected, Target::Subscriber) => {},
            _ => {}
        }
        Ok(())
    }
    
    async fn handle_ws_message(&mut self, ctx: &mut Ctx<'_, Self>, message: Result<Message, Error>) -> Result<(), Error> {
        let text = match message? {
            Message::Close(_) => {
                self.stop(ctx).await;
                return Ok(());
            },
            Message::Text(text) => text,
            _ => return Ok(())
        };
        let message: SignalMessage = serde_json::from_str(&text)?;
        match message {
            SignalMessage::DeviceType { device_type } => {
                let _ = self.publisher.strong().send(PublisherMessage::InitiateMonitoring { device_type }).await;
            },
            SignalMessage::Rtc { target, message_type } => {
                match target {
                    Target::Publisher => {
                        let _ = self.publisher.strong().send(PublisherMessage::Websocket(message_type)).await;
                    }
                    Target::Subscriber => {
                        let _ = self.subscriber.strong().send(SubscriberMessage::Websocket(message_type)).await;
                    }
                }
            }
        }
        Ok(())
    }
    async fn send_ws_message(&mut self, msg: SignalMessage) -> Result<(), Error> {
        let signaling_message = serde_json::to_string(&msg)?;
        self.ws_tx.send(Message::Text(signaling_message.into())).await?;
        Ok(())
    }
    async fn unsubscribe(&mut self, speaker_id: Uuid) -> Result<(), Error> {
        let _ = self.subscriber.strong().send(SubscriberMessage::Unsubscribe { from: speaker_id }).await;
        let notice = serde_json::json!({
            "type": "peer_left",
            "peer_id": speaker_id
        });
        self.ws_tx.send(Message::Text(notice.to_string().into())).await?;
        Ok(())
    }
}

async fn initiate_ice_restart(
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