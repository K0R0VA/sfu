use std::{collections::HashMap, sync::Arc};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, stream::SplitSink};
use uuid::Uuid;
use webrtc::{peer_connection::{RTCPeerConnection, sdp::session_description::RTCSessionDescription}, rtp::packet::Packet, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_sender::RTCRtpSender, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::{TrackLocalWriter, track_local_static_rtp::TrackLocalStaticRTP}};
use crate::{actor::{Actor, Addr, Ctx}, error::Error, room::{Room, RoomMessage}};

pub struct User {
    pub room: Addr<Room>,
    pub peer_id: Uuid,
    pub ws_tx: SplitSink<WebSocket, Message>,
    pub peer_connection: Arc<RTCPeerConnection>,
    pub subscriptions: HashMap<Uuid, Subscription>
}

pub struct Subscription {
    sender: Arc<RTCRtpSender>,
    drop: tokio::sync::oneshot::Sender<()>
}

pub enum UserMessage {
    Websocket {
        message: Result<Message, Error>
    },
    ConnectToUser {
        speaker_id: Uuid,
        stream: tokio::sync::broadcast::Receiver<Packet>,
        codec_mime_type: String,
    },
    DisconnectFromUser {
        speaker_id: Uuid,
    },
    RoomClosed
}

impl Actor for User {
    type Message = UserMessage;
    async fn handle(&mut self, ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            UserMessage::Websocket { message } => {
                if let Err(e) = self.handle_ws_message(message).await {
                    println!("handle_ws_message failed {:?}", e);
                    self.stop(ctx).await;
                }
            },
            UserMessage::ConnectToUser { speaker_id, stream, codec_mime_type } => {
                if let Err(e) = self.connect_to_user(speaker_id, stream, codec_mime_type).await {
                    println!("connect_to_user failed {:?}", e);
                    self.stop(ctx).await;
                }
                println!("👤 [UserActor] Участник {} подписался на {}", self.peer_id, speaker_id);
            },
            UserMessage::RoomClosed => self.stop(ctx).await,
            UserMessage::DisconnectFromUser { speaker_id } => {
                if let Err(e) = self.disconnect_from_user(speaker_id).await {
                    println!("disconnect_from_user failed {:?}", e);
                    self.stop(ctx).await;
                }
            }
        }
    }   
    async fn starting(&mut self, _: &Ctx<'_, Self>) {
        println!("🟢 [Userctor] Пользователь инициализирован.");
    }
    async fn stopping(&mut self, _: &Ctx<'_, Self>) {
        let _ = self.room.send(RoomMessage::Leave { peer_id: self.peer_id.clone() }).await;
        println!("🔴 [UserActor] Пользователь уничтожен.");
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
    async fn connect_to_user(&mut self, speaker_id: Uuid, stream: tokio::sync::broadcast::Receiver<Packet>, codec_mime_type: String) -> Result<(), Error> {
        let output_track: Arc<TrackLocalStaticRTP> = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: codec_mime_type,
                ..Default::default()
            },
            "video".to_owned(),
            speaker_id.to_string(),
        ));
        let sender = self.peer_connection.add_transceiver_from_track(
                output_track.clone() as Arc<_>,
                Some(RTCRtpTransceiverInit { direction: RTCRtpTransceiverDirection::Sendonly, send_encodings: vec![] })
            )
            .await?
            .sender()
            .await;
        let (drop, is_dropped) = tokio::sync::oneshot::channel();
        self.subscriptions.insert(speaker_id, Subscription { sender, drop });
        let peer_id = self.peer_id;
        tokio::spawn(async move {
            tokio::select! {
                _ = handle_rtp_packets(peer_id, stream, output_track) => {},
                _ = is_dropped => {}
            }
        });
        let notice = serde_json::json!({
            "type": "peer_joined",
            "peer_id": speaker_id
        });
        self.ws_tx.send(Message::Text(notice.to_string().into())).await?;
        Ok(())
    }
    async fn disconnect_from_user(&mut self, speaker_id: Uuid) -> Result<(), Error> {
        let Subscription { sender, drop } = self.subscriptions.remove(&speaker_id).unwrap();
        let _ = drop.send(());
        self.peer_connection.remove_track(&sender).await?;
        let notice = serde_json::json!({
            "type": "peer_left",
            "peer_id": speaker_id
        });
        self.ws_tx.send(Message::Text(notice.to_string().into())).await?;
        Ok(())
    }
}

async fn handle_rtp_packets(peer_id: Uuid, mut stream: tokio::sync::broadcast::Receiver<Packet>, output_track: Arc<TrackLocalStaticRTP>) {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match stream.recv().await {
            // Сценарий 1: Пакет успешно получен — шлем его в сеть!
            Ok(packet) => {
                if let Err(_) = output_track.write_rtp(&packet).await {
                    println!("❌ Сетевое соединение с {} закрылось, тушим бродкаст", peer_id);
                    break; 
                }
            }
            Err(RecvError::Lagged(skipped_packets)) => {
                println!("⚠️ [SFU БУФЕР] Читатель {} отстал! Пропущено пакетов: {}", peer_id, skipped_packets);
            }
            Err(RecvError::Closed) => {
                println!("🛑 Стример закрыл трансляцию, выходим.");
                break;
            }
        }
    }
}
