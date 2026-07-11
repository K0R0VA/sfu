use std::{sync::{Arc, atomic::{AtomicU8, Ordering::Relaxed}}};

use sfu::{actor::{Actor, StreamItem::Next}, error::Error, room::{RoomMessage}, server::{Server, ServerMessage}, user::User};

use crate::spawn_test_client;

#[tokio::test(flavor = "multi_thread")]
async fn connect_one_user_to_room() -> Result<(), Error> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter("webrtc=ERROR,webrtc_ice=ERROR")
        .init();
    let server: sfu::actor::Addr<Server<_>> = Server::default().start();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = server.send(ServerMessage::CreateRoom { name: "".to_string(), response_channel: tx }).await;
    let (_, room) = rx.await.unwrap();
    let (channel, mut client, stream) = spawn_test_client().await?;
    let user = User::new(channel, room.clone()).await?;
    let user_id = user.peer_id;
    let addr = user.start();
    addr.add_stream(tokio_stream::wrappers::ReceiverStream::new(stream), 
    |m| Next(sfu::user::UserMessage::SyncMessage(sfu::user::SyncMessage::Message(m))));
    let _ = room.send(RoomMessage::Join { peer_id: user_id, addr }).await;
    client.setup().await?;
    for _stage in 0 .. 2 {
        client.handle_message().await?;
    } 
    assert_eq!(client.markers.send_offer, true);
    assert_eq!(client.markers.receive_answer, true);
    Ok(())
}

#[tokio::test]
async fn connect_two_users_to_room() -> Result<(), Error> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter("webrtc=ERROR,webrtc_ice=ERROR")
        .init();
    let server: sfu::actor::Addr<Server<_>> = Server::default().start();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = server.send(ServerMessage::CreateRoom { name: "".to_string(), response_channel: tx }).await;
    let (_, room) = rx.await.unwrap();
    let counter = Arc::new(AtomicU8::default());
    let task_1 = {
        let (channel, mut client, stream) = spawn_test_client().await?;
        let user = User::new(channel, room.clone()).await?;
        let peer_id = user.peer_id;
        let user = user.start();
        user.add_stream(tokio_stream::wrappers::ReceiverStream::new(stream), 
        |m| Next(sfu::user::UserMessage::SyncMessage(sfu::user::SyncMessage::Message(m))));
        let _ = room.send(RoomMessage::Join { peer_id, addr: user }).await;
        let counter = counter.clone();
        tokio::spawn(async move {
            client.setup().await?;
            for _stage in 0 .. 4 {
                client.handle_message().await?;
            } 
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            while counter.load(Relaxed) != 2 {
                tokio::task::yield_now().await;
            }   
            assert_eq!(client.markers.send_offer, true);
            assert_eq!(client.markers.ice_connected, true);
            assert_eq!(client.markers.receive_answer, true);
            assert_eq!(client.markers.receive_offer, true);
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
            client.setup().await?;
            for _stage in 0 .. 4 {
                client.handle_message().await?;
            } 
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            while counter.load(Relaxed) != 2 {
                tokio::task::yield_now().await;
            }   
            assert_eq!(client.markers.send_offer, true);
            assert_eq!(client.markers.ice_connected, true);
            assert_eq!(client.markers.receive_answer, true);
            assert_eq!(client.markers.receive_offer, true);
            Result::<_, Error>::Ok(())
        })
    };
    let (r1, r2) = tokio::join!(task_1, task_2);
    r1.unwrap()?;
    r2.unwrap()?;
    Ok(())
}

#[tokio::test]
async fn connect_many_users_to_room() -> Result<(), Error> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter("webrtc=ERROR,webrtc_ice=ERROR")
        .init();
    let server: sfu::actor::Addr<Server<_>> = Server::default().start();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = server.send(ServerMessage::CreateRoom { name: "".to_string(), response_channel: tx }).await;
    let (_, room) = rx.await.unwrap();
    let mut tasks = Vec::with_capacity(24);
    let counter = Arc::new(AtomicU8::default());
    for _ in 0 .. 24 {
        let (channel, mut client, stream) = spawn_test_client().await?;
        let user = User::new(channel, room.clone()).await?;
        let peer_id = user.peer_id;
        let user = user.start();
        user.add_stream(tokio_stream::wrappers::ReceiverStream::new(stream), 
        |m| Next(sfu::user::UserMessage::SyncMessage(sfu::user::SyncMessage::Message(m))));
        let _ = room.send(RoomMessage::Join { peer_id, addr: user }).await;
        let counter = counter.clone();
        let task = tokio::spawn(async move {
            client.setup().await?;
            for _stage in 0 .. 4 {
                client.handle_message().await?;
            } 
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            while counter.load(Relaxed) != 24 {
                tokio::task::yield_now().await;
            }   
            assert_eq!(client.markers.send_offer, true);
            assert_eq!(client.markers.ice_connected, true);
            assert_eq!(client.markers.receive_answer, true);
            assert_eq!(client.markers.receive_offer, true);
            Result::<_, Error>::Ok(())
        });
        tasks.push(task);
    }
    futures_util::future::try_join_all(tasks).await.unwrap();
    Ok(())
}