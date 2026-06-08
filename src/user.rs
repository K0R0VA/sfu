use std::{collections::HashMap, sync::{Arc}};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, stream::SplitSink};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use webrtc::{ice_transport::ice_candidate::RTCIceCandidateInit, peer_connection::{RTCPeerConnection, sdp::{session_description::RTCSessionDescription}}, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::{TrackLocalWriter, track_local_static_rtp::TrackLocalStaticRTP}};
use crate::{PacketSubscription, actor::{Actor, Addr, Ctx}, audio_subscription::AudioSubscription, error::Error, room::{Room, RoomMessage, StreamQuality}, video_subscription::{VideoSubscription, VideoSubscriptionMessage}};

pub struct User {
    pub room: Addr<Room>,
    pub peer_id: Uuid,
    pub ws_tx: SplitSink<WebSocket, Message>,
    pub publisher_pc: Arc<RTCPeerConnection>,
    pub subscriber_pc: Arc<RTCPeerConnection>,
    pub audio_subscriptions: HashMap<Uuid, Addr<AudioSubscription>>,
    pub video_subscriptions: HashMap<Uuid, Addr<VideoSubscription>>
}


pub enum UserMessage {
    IceCandidate { candidate: String, target: Target },
    Websocket {
        message: Result<Message, Error>
    },
    ConnectAudio(ConnectionRequest),
    ConnectVideo { request: ConnectionRequest, quality: StreamQuality },
    DisconnectFromUser {
        speaker_id: Uuid,
    },
    RoomClosed
}

pub struct ConnectionRequest {
    pub speaker_id: Uuid,
    pub stream: PacketSubscription,
    pub codec_mime_type: String,
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
            UserMessage::IceCandidate { candidate, target } => {
                if let Err(e) = self.send_ice_candidate(target, candidate).await {
                    tracing::error!("[UserActor] UserMessage::IceCandidate {e}");
                }
            }
            UserMessage::Websocket { message } => {
                if let Err(e) = self.handle_ws_message(message).await {
                    tracing::error!("👤 [UserActor] UserMessage::Websocket {:?}", e);
                    self.stop(ctx).await;
                }
            },
            UserMessage::ConnectAudio(request) => {
                if let Err(e) = self.connect_audio(ctx.addr.clone(), request).await {
                    tracing::error!("👤 [UserActor] UserMessage::ConnectToUser {:?}", e);
                    self.stop(ctx).await;
                }
            },
            UserMessage::ConnectVideo {quality, request} => {
                if let Err(e) = self.connect_video(ctx.addr.clone(), quality, request).await {
                    tracing::error!("👤 [UserActor] UserMessage::ConnectToUser {:?}", e);
                    self.stop(ctx).await;
                }
            },
            UserMessage::RoomClosed => self.stop(ctx).await,
            UserMessage::DisconnectFromUser { speaker_id } => {
                if let Err(e) = self.disconnect_from_user(speaker_id).await {
                    tracing::error!("👤 [UserActor] UserMessage::DisconnectFromUser {:?}", e);
                    self.stop(ctx).await;
                }
            }
        }
    }   
    async fn starting(&mut self, ctx: &Ctx<'_, Self>) {
        let addr = ctx.addr.clone();
        self.publisher_pc.on_ice_candidate(Box::new(move |candidate| {
            let addr = addr.clone();
            Box::pin(async move {
                if let Some(cand) = candidate {
                    if let Ok(cand_json) = cand.to_json() {
                        let _ = addr.send(UserMessage::IceCandidate { candidate: cand_json.candidate, target: Target::Publisher }).await;
                    }
                }
            })
        }));
        let addr = ctx.addr.clone();
        self.subscriber_pc.on_ice_candidate(Box::new(move |candidate| {
            let addr = addr.clone();
            Box::pin(async move {
                if let Some(cand) = candidate {
                    if let Ok(cand_json) = cand.to_json() {
                        let _ = addr.send(UserMessage::IceCandidate { candidate: cand_json.candidate, target: Target::Subscriber }).await;
                    }
                }
            })
        }));
        tracing::info!("🟢 [Userctor] Пользователь инициализирован.");
    }
    async fn stopping(&mut self, _: &Ctx<'_, Self>) {
        if let Err(e) = self.publisher_pc.close().await {
            tracing::error!("[UserActor] publisher_pc.close() {e}");
        }
        if let Err(e) = self.subscriber_pc.close().await {
            tracing::error!("[UserActor] publisher_pc.close() {e}");
        }
        let _ = self.room.send(RoomMessage::Leave { peer_id: self.peer_id.clone() }).await;
        tracing::info!("🔴 [UserActor] Пользователь уничтожен.");
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SignalMessage {
    target: Target,
    #[serde(flatten)] 
    message_type: MessageType,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")] 
pub enum MessageType {
    Offer { sdp: String },
    Answer { sdp: String },
    Candidate { candidate: String }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Target {
    Publisher,
    Subscriber
}

impl User {
    async fn handle_ws_message(&mut self, message: Result<Message, Error>) -> Result<(), Error> {
        let text = match message? {
            Message::Close(_) => {
                let _ = self.room.send(RoomMessage::Leave { peer_id: self.peer_id.clone() }).await;
                return Ok(());
            },
            Message::Text(text) => text,
            _ => return Ok(())
        };
        let sdp: SignalMessage = serde_json::from_str(&text).map_err(|e| {
            println!("{text}");
            e
        })?;
        match (sdp.target, sdp.message_type) {
            (Target::Publisher, MessageType::Offer {sdp}) => {
                let offer_desc = RTCSessionDescription::offer(sdp)?;
                self.publisher_pc.set_remote_description(offer_desc).await?;
                let answer = self.publisher_pc.create_answer(None).await?;
                 let mut gather_complete = self.publisher_pc.gathering_complete_promise().await;
                self.publisher_pc.set_local_description(answer.clone()).await?;
                let _ = gather_complete.recv().await; //
                let message = SignalMessage {
                    target: Target::Publisher,
                    message_type: MessageType::Answer {sdp: answer.sdp }
                };
                let json_answer = serde_json::to_string(&message)?;
                self.ws_tx.send(Message::Text(json_answer.into())).await?;
            },
            (Target::Subscriber, MessageType::Answer { sdp }) => {
                let answer_desc = RTCSessionDescription::answer(sdp)?;
                self.subscriber_pc.set_remote_description(answer_desc).await?;
            },
            (Target::Publisher, MessageType::Candidate { candidate }) => {
                let candidate_init = RTCIceCandidateInit {
                    candidate,
                    ..Default::default()
                };
                self.publisher_pc.add_ice_candidate(candidate_init).await?;
            },
            (Target::Subscriber, MessageType::Candidate { candidate }) => {
                let candidate_init = RTCIceCandidateInit {
                    candidate,
                    ..Default::default()
                };
                self.subscriber_pc.add_ice_candidate(candidate_init).await?;
            },
            (target, r#type) => {
                println!("{:?} {:?}",  target, r#type);
            }
        }
        Ok(())
    }
    async fn connect_audio(&mut self, user: Addr<Self>, request: ConnectionRequest) -> Result<(), Error> {
        let peer_id = request.speaker_id;
        let audio_subscription = AudioSubscription::init(self.subscriber_pc.clone(), user, request).await?;
        let audio_subscription = audio_subscription.start();
        self.audio_subscriptions.insert(peer_id, audio_subscription);
        self.create_offer().await?;
        Ok(())
    }
    async fn connect_video(&mut self, user: Addr<Self>, quality: StreamQuality, request: ConnectionRequest) -> Result<(), Error> {
        let peer_id = request.speaker_id;
        match self.video_subscriptions.entry(peer_id) {
            std::collections::hash_map::Entry::Occupied(o) => {
                let addr = o.get();
                let _ = addr.send(VideoSubscriptionMessage::AddSubsription { quality, stream: request.stream }).await;
            },
            std::collections::hash_map::Entry::Vacant(v) => {
                let video_subscription = VideoSubscription::new(
                    self.subscriber_pc.clone(), 
                    peer_id, 
                    request.codec_mime_type, 
                    request.stream, 
                    quality
                ).await?;
                let video_subscription = video_subscription.start();
                v.insert(video_subscription);
                self.create_offer().await?;
            }
        };
        Ok(())
    }
    async fn create_offer(&mut self) -> Result<(), Error> {
        let offer = self.subscriber_pc.create_offer(None).await?;
        // 2. Устанавливаем его как LocalDescription на сервере
        self.subscriber_pc.set_local_description(offer.clone()).await?;

        // 3. Формируем сообщение для отправки по WebSocket
        let signaling_message = serde_json::json!(SignalMessage {
            target: Target::Subscriber,
            message_type: MessageType::Offer { sdp: offer.sdp }
        });
        self.ws_tx.send(Message::Text(signaling_message.to_string().into())).await?;
        Ok(())
    }
    async fn send_ice_candidate(&mut self, target: Target, candidate: String) -> Result<(), Error> {
        let signaling_message = SignalMessage {
            target,
            message_type: MessageType::Candidate { candidate }
        };
        let str = serde_json::to_string(&signaling_message)?;
        self.ws_tx.send(Message::Text(str.into())).await?;
        Ok(())
    }
    async fn disconnect_from_user(&mut self, speaker_id: Uuid) -> Result<(), Error> {
        // let subscription = self.subscriptions
        //     .remove(&speaker_id)
        //     .ok_or(Error::SystemError { message: "subscription not found".into() })?;
        // subscription.disconnect(&self.peer_connection).await?;
        
        let notice = serde_json::json!({
            "type": "peer_left",
            "peer_id": speaker_id
        });
        self.ws_tx.send(Message::Text(notice.to_string().into())).await?;
        Ok(())
    }
}