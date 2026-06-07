use std::{collections::HashMap, sync::{Arc, atomic::Ordering}};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, stream::SplitSink};
use uuid::Uuid;
use webrtc::{peer_connection::{RTCPeerConnection, sdp::session_description::RTCSessionDescription}, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::{RTCRtpCodecCapability}, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::{TrackLocalWriter, track_local_static_rtp::TrackLocalStaticRTP}};
use crate::{PacketStream, PacketSubscription, actor::{Actor, Addr, Ctx}, error::Error, room::{Room, RoomMessage, StreamQuality}, video_subscription::{VideoSubscription, VideoSubscriptionMessage}};

pub struct User {
    pub room: Addr<Room>,
    pub peer_id: Uuid,
    pub ws_tx: SplitSink<WebSocket, Message>,
    pub peer_connection: Arc<RTCPeerConnection>,
    pub subscriptions: HashMap<Uuid, Subscription>
}

pub struct Subscription {
    audio_stream: Option<AudioSubscription>,
    video_subscription: Option<Addr<VideoSubscription>>,
    notified: bool,
}

impl Subscription {
    async fn init(pc: Arc<RTCPeerConnection>, request: ConnectionRequest) -> Result<Self, Error> {
        let mut video_subscription = None;
        let mut audio_stream= None;
        let kind = request.kind;
        match kind {
            ConnectionRequestKind::Audio => { 
                let inner = AudioSubscription::init(&pc, request).await?;
                let _ = audio_stream.insert(inner); 
            },
            ConnectionRequestKind::Video { stream_quality } => { 
                let ConnectionRequest { speaker_id, stream, codec_mime_type, .. } = request;
                let addr = VideoSubscription::new(pc, speaker_id, codec_mime_type, stream, stream_quality)
                    .await?
                    .start();
                video_subscription = Some(addr);
            },
        };
        Ok(Self {
            video_subscription,
            audio_stream,
            notified: false
        })
    }
    async fn apply_request(&mut self, pc: Arc<RTCPeerConnection>, request: ConnectionRequest) -> Result<(), Error> {
        match request.kind {
            ConnectionRequestKind::Audio if self.audio_stream.is_none() =>{
                let audio_subscription = AudioSubscription::init(&pc, request).await?;
                let _ = self.audio_stream.insert(audio_subscription);
            },
            ConnectionRequestKind::Video { stream_quality } if self.video_subscription.is_none() => {
                let ConnectionRequest { speaker_id, stream, codec_mime_type, .. } = request;
                let addr = VideoSubscription::new(pc, speaker_id, codec_mime_type, stream, stream_quality)
                    .await?
                    .start();
                self.video_subscription = Some(addr);
            },
            ConnectionRequestKind::Video { stream_quality } => {
                if let Some(video_subscription) = &self.video_subscription {
                    let ConnectionRequest { stream, .. } = request;
                    let _ = video_subscription.send(VideoSubscriptionMessage::AddSubsription {
                        quality: stream_quality, 
                        stream
                    }).await;
                }
            },
            _ => return Ok(()),
        };
        Ok(())
    }

    async fn disconnect(mut self, pc: &RTCPeerConnection) -> Result<(), Error> {
        if let Some(video_subscription) = self.video_subscription.take() {
            let _ = video_subscription.send(VideoSubscriptionMessage::Drop).await;
        }
        if let Some(AudioSubscription { sender, drop }) = self.audio_stream.take() {
            let _ = drop.send(());
            pc.remove_track(&sender).await?;
        }
        Ok(())
    }
}

pub struct AudioSubscription {
    sender: Arc<RTCRtpSender>,
    drop: tokio::sync::oneshot::Sender<()>
}


impl AudioSubscription {
    pub async fn init(pc: &RTCPeerConnection, ConnectionRequest {codec_mime_type, speaker_id, stream, ..}: ConnectionRequest) -> Result<Self, Error> {
        let output_track: Arc<TrackLocalStaticRTP> = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: codec_mime_type,
                ..Default::default()
            },
            Uuid::new_v4().to_string(),
            speaker_id.to_string(),
        ));
        let sender = pc.add_transceiver_from_track(
                output_track.clone() as Arc<_>,
                Some(RTCRtpTransceiverInit { direction: RTCRtpTransceiverDirection::Sendonly, send_encodings: vec![] })
            )
            .await?
            .sender()
            .await;
        let (drop, is_dropped) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let PacketSubscription { mut stream, active_receiver_counter } = stream;
            active_receiver_counter.fetch_add(1, Ordering::Relaxed);
            tokio::select! {
                result = handle_rtp_packets(&mut stream, output_track) => {
                    if let Err(e) = result {
                        tracing::error!("[AudioSubscription] {e}");
                    }
                },
                _ = is_dropped => {}
            }
            active_receiver_counter.fetch_sub(1, Ordering::Relaxed);
        });
        Ok(Self {
            drop,
            sender
        })
    }
}

pub enum UserMessage {
    Websocket {
        message: Result<Message, Error>
    },
    ConnectToUser(ConnectionRequest),
    DisconnectFromUser {
        speaker_id: Uuid,
    },
    RoomClosed
}

pub struct ConnectionRequest {
    pub speaker_id: Uuid,
    pub stream: PacketSubscription,
    pub codec_mime_type: String,
    pub kind: ConnectionRequestKind
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
            UserMessage::Websocket { message } => {
                if let Err(e) = self.handle_ws_message(message).await {
                    tracing::error!("👤 [UserActor] UserMessage::Websocket {:?}", e);
                    self.stop(ctx).await;
                }
            },
            UserMessage::ConnectToUser(request) => {
                if let Err(e) = self.connect_to_user(request).await {
                    tracing::error!("👤 [UserActor] UserMessage::ConnectToUser {:?}", e);
                    self.stop(ctx).await;
                }
                // tracing::info!("👤 [UserActor] Участник {} подписался на {}", self.peer_id, speaker_id);
            },
            UserMessage::RoomClosed => self.stop(ctx).await,
            UserMessage::DisconnectFromUser { speaker_id } => {
                if let Err(e) = self.disconnect_from_user(speaker_id).await {
                    tracing::error!("👤 [UserActor] UserMessage::DisconnectFromUser {:?}", e);
                    self.stop(ctx).await;
                }
                //  tracing::info!("👤 [UserActor] Участник {} отписался от {}", self.peer_id, speaker_id);
            }
        }
    }   
    async fn starting(&mut self, _: &Ctx<'_, Self>) {
        tracing::info!("🟢 [Userctor] Пользователь инициализирован.");
    }
    async fn stopping(&mut self, _: &Ctx<'_, Self>) {
        let _ = self.room.send(RoomMessage::Leave { peer_id: self.peer_id.clone() }).await;
        tracing::info!("🔴 [UserActor] Пользователь уничтожен.");
    }
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
        let sdp: RTCSessionDescription = serde_json::from_str(&text)?;
        self.peer_connection.set_remote_description(sdp).await?;
        let answer = self.peer_connection.create_answer(None).await?;
        let mut gather_complete = self.peer_connection.gathering_complete_promise().await;
        self.peer_connection.set_local_description(answer).await?;
        let _ = gather_complete.recv().await; // Ждем сбора всех ICE-кандидатов
        if let Some(local_desc) = self.peer_connection.local_description().await {
            let json_answer = serde_json::to_string(&local_desc)?;
            self.ws_tx.send(Message::Text(json_answer.into())).await?;
        }
        Ok(())
    }
    async fn connect_to_user(&mut self, request: ConnectionRequest) -> Result<(), Error> {
        match self.subscriptions.entry(request.speaker_id) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                let subscriprion = o.get_mut();
                subscriprion.apply_request(self.peer_connection.clone(), request).await?;
                if subscriprion.audio_stream.is_some() && subscriprion.video_subscription.is_some() && !subscriprion.notified{
                    let notice = serde_json::json!({
                        "type": "peer_join",
                    });
                    self.ws_tx.send(Message::Text(notice.to_string().into())).await?;
                    subscriprion.notified = true;
                }
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                let subscription = Subscription::init(self.peer_connection.clone(), request).await?;
                v.insert(subscription);
            }
        }
        Ok(())
    }
    async fn disconnect_from_user(&mut self, speaker_id: Uuid) -> Result<(), Error> {
        let subscription = self.subscriptions
            .remove(&speaker_id)
            .ok_or(Error::SystemError { message: "subscription not found".into() })?;
        subscription.disconnect(&self.peer_connection).await?;
        let notice = serde_json::json!({
            "type": "peer_left",
            "peer_id": speaker_id
        });
        self.ws_tx.send(Message::Text(notice.to_string().into())).await?;
        Ok(())
    }
}

pub async fn handle_rtp_packets(stream: &PacketStream, output_track: Arc<TrackLocalStaticRTP>) -> Result<(), Error> {
    let mut stream = stream.resubscribe();
    loop {
        let packet = match stream.recv().await {
            Ok(packet) => packet,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(missed_packets)) => {
                tracing::warn!("miss {missed_packets} packets");
                continue;
            },
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return Err(Error::Broadcast(tokio::sync::broadcast::error::RecvError::Closed))
        };
        output_track.write_rtp(&packet).await?;
    }
}