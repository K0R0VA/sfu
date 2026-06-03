use futures_util::StreamExt;
use tokio::sync::mpsc::error::SendError;

pub trait Actor: Sized + Send + 'static {
    type Message: Sized + Send + 'static;
    fn handle(&mut self, ctx: &Ctx<'_, Self>, m: Self::Message) -> impl Future<Output = ()> + Send;
    fn starting(&mut self, ctx: &Ctx<'_, Self>) -> impl Future<Output = ()> + Send;
    fn stop(&mut self) -> impl Future<Output = ()> + Send;
    fn start(mut self) -> Addr<Self> {
        let (requests, mut messages) = tokio::sync::mpsc::channel(128);
        let addr = Addr { requests };
        tokio::spawn({
            let ctx_addr = addr.clone();
            async move {
                let ctx = Ctx {addr: &ctx_addr};
                self.starting(&ctx).await;
                while let Some(message) = messages.recv().await {
                    self.handle(&ctx, message).await;
                }
                self.stop().await
            }
        });
        addr
    }
}

pub struct Ctx< 'a, A: Actor,> {
    pub addr: &'a Addr<A>
}

 pub struct Addr<A: Actor> {
    requests: tokio::sync::mpsc::Sender<<A as Actor>::Message>,
}

impl<A: Actor> Clone for Addr<A> {
    fn clone(&self) -> Self {
        Addr {
            requests: self.requests.clone()
        }
    }
}

impl<A: Actor> Addr<A> {
    pub async fn send(&self, m: <A as Actor>::Message) -> Result<(), SendError<<A as Actor>::Message>> {
        self.requests.send(m).await?;
        Ok(())
    }
    pub fn add_stream<S, F>(&self, mut stream: S, mapper: F)
    where
        S: futures_util::Stream + Send + Unpin + 'static,
        F: Fn(S::Item) -> StreamItem<A::Message> + Send + Sync + 'static,
        S::Item: Send + Sync
    {
        let addr = self.clone();
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                // Превращаем элемент стрима в сообщение актора и отправляем
                let StreamItem::Next(msg) = mapper(item) else { break; };
                if addr.send(msg).await.is_err() {
                    break; // Актор умер, выходим из цикла
                }
            }
        });
    }
}

pub enum StreamItem<T> {
    Next(T),
    Close
}


#[cfg(test)]
mod tests {
    use std::time::Duration;

use crate::actor::{Actor, Ctx};

    pub struct TestActor;

    impl Actor for TestActor {
        type Message = ();
        async fn starting(&mut self, ctx: &Ctx<'_, Self>) {
            println!("starting");
        }
        async fn stop(&mut self) {
            println!("stopped");
        }
        async fn handle(&mut self, ctx: &Ctx<'_, Self>, m: Self::Message) {
            println!("handle message")
        }
    }

   

    #[tokio::test]
    async fn test() {
        let actor = TestActor;
        let addr = actor.start();
        let _ = addr.send(()).await;
        drop(addr);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}