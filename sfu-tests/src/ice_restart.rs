use std::time::Duration;

use sfu::{actor::{Actor, StreamItem::Next}, error::Error, room::{Room, RoomMessage}, user::User};

use crate::spawn_test_client;

#[tokio::test]
async fn ice_restart() -> Result<(), Error> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter("webrtc=ERROR,webrtc_ice=ERROR,api_gateway=INFO,sfu=INFO")
        .init();
    let room = Room::default().start();
    let task_1 = {
        let (channel, mut client, stream) = spawn_test_client().await?;
        let user = User::new(channel, room.clone()).await?;
        let peer_id = user.peer_id;
        let user = user.start();
        user.add_stream(tokio_stream::wrappers::ReceiverStream::new(stream), 
        |m| Next(sfu::user::UserMessage::SyncMessage(sfu::user::SyncMessage::Message(m))));
        let _ = room.send(RoomMessage::Join { peer_id, addr: user }).await;
        tokio::spawn(async move {
            loop {
                client.setup().await?;
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        assert_ne!(client.peer_id, None);
                        break;
                    }
                    r = client.run_loop() => {
                        assert_ne!(true, r.is_err())
                    }

                }
            }
            Result::<_, Error>::Ok(())
        })
    };
    let task_2 = {
        let (channel, mut client, stream) = spawn_test_client().await?;
        let user = User::new(channel, room.clone()).await?;
        let peer_id = user.peer_id;
        let user = user.start();
        user.add_stream(tokio_stream::wrappers::ReceiverStream::new(stream), 
        |m| Next(sfu::user::UserMessage::SyncMessage(sfu::user::SyncMessage::Message(m))));
        let _ = room.send(RoomMessage::Join { peer_id, addr: user }).await;
        tokio::spawn(async move {
            loop {
                client.setup().await?;
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        assert_ne!(client.peer_id, None);
                        break;
                    }
                    r = client.run_loop() => {
                        return r;
                    }

                }
            }
            Result::<_, Error>::Ok(())
        })
    };
    let (r1, r2) = tokio::join!(task_1, task_2);
    r1.unwrap()?;
    r2.unwrap()?;
    Ok(())
}