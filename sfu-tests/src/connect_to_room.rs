use std::time::Duration;

use sfu::{actor::{Actor, StreamItem::Next}, error::Error, room::Room, user::User};

use crate::spawn_test_client;

#[tokio::test]
async fn connect_one_user_to_room() -> Result<(), Error> {
    tracing_subscriber::fmt().init();
    let room = Room::default().start();
    let (channel, mut client, stream) = spawn_test_client().await?;
    let user = User::new(channel, room).await?.start();
    user.add_stream(tokio_stream::wrappers::ReceiverStream::new(stream), 
    |m| Next(sfu::user::UserMessage::SyncMessage(sfu::user::SyncMessage::Message(m))));
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
    Ok(())
}

#[tokio::test]
async fn connect_two_users_to_room() -> Result<(), Error> {
    tracing_subscriber::fmt().init();
    let room = Room::default().start();
    let task_1 = {
        let (channel, mut client, stream) = spawn_test_client().await?;
        let user = User::new(channel, room.clone()).await?.start();
        user.add_stream(tokio_stream::wrappers::ReceiverStream::new(stream), 
        |m| Next(sfu::user::UserMessage::SyncMessage(sfu::user::SyncMessage::Message(m))));
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
        let user = User::new(channel, room).await?.start();
        user.add_stream(tokio_stream::wrappers::ReceiverStream::new(stream), 
        |m| Next(sfu::user::UserMessage::SyncMessage(sfu::user::SyncMessage::Message(m))));
        tokio::spawn(async move {
            loop {
                client.setup().await?;
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        assert_ne!(client.peer_id, None);
                        break;
                    }
                    r = client.run_loop() => {
                        tracing::error!("{:?}", r.err());
                        break;
                    }

                }
            }
            Result::<_, Error>::Ok(())
        })
    };
    let (_, _) = tokio::join!(task_1, task_2);
    Ok(())
}