use std::time::Duration;
use sfu::{actor::{Actor, StreamItem::Next}, error::Error, room::RoomMessage, server::{Server, ServerMessage}, user::{SessionParams, User}};
use uuid::Uuid;
use crate::{FileStorage, spawn_test_client};

#[tokio::test]
async fn ice_restart() -> Result<(), Error> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter("webrtc=ERROR,webrtc_ice=ERROR,api_gateway=INFO,sfu=INFO")
        .init();
    let server: sfu::actor::Addr<Server<_, FileStorage>> = Server::default().start();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = server.send(ServerMessage::CreateRoom { name: "".to_string(), response_channel: tx });
    let (_, room) = rx.await.unwrap();
    let (channel, mut client, stream) = spawn_test_client().await?;
    let user_id = Uuid::new_v4();
    let user = User::new(user_id,channel, SessionParams { 
        device: sfu::quality_monitor::DeviceType::Desktop,
        user_id: Some(user_id),
    }, room.clone()).await?;
    let user = user.start();
    user.add_stream(tokio_stream::wrappers::ReceiverStream::new(stream), 
    |m| Next(sfu::user::UserMessage::SyncMessage(sfu::user::SyncMessage::Message(m))));
    let _ = room.send(RoomMessage::Join { peer_id: user_id, addr: user }).await;
        loop {
            client.setup().await?;
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    assert_ne!(client.peer_id, None);
                    break;
                }
                r = client.handle_message() => {
                    assert_ne!(true, r.is_err())
                }

            }
        }
    Ok(())
}