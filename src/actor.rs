use futures_util::StreamExt;
use tokio::sync::mpsc::error::SendError;

pub trait Actor: Sized + Send + 'static {
    type Message: Sized + Send + 'static;
    fn handle(&mut self, ctx: &mut Ctx<'_, Self>, m: Self::Message) -> impl Future<Output = ()> + Send;
    fn starting(&mut self, ctx: &Ctx<'_, Self>) -> impl Future<Output = ()> + Send;
    fn stopping(&mut self, ctx: &Ctx<'_, Self>) -> impl Future<Output = ()> + Send;
    fn stop(&mut self, ctx: &mut Ctx<'_, Self>) -> impl Future<Output = ()> + Send {
        async move {
            ctx.should_stop = true;
            self.stopping(ctx).await;
        }
    }
    fn start(self) -> Addr<Self> {
        Addr::spawn(self, 32)
    }
    fn start_with_capacity(self, capacity: usize) -> Addr<Self> {
        Addr::spawn(self, capacity)
    }
}

pub struct Ctx< 'a, A: Actor,> {
    pub addr: &'a Addr<A>,
    should_stop: bool 
}

async fn handle_messages<A: Actor>(actor: &mut A, ctx: &mut Ctx<'_, A>, mut messages: tokio::sync::mpsc::Receiver<A::Message>) {
    while let Some(message) = messages.recv().await {
        actor.handle(ctx, message).await;
        if ctx.should_stop {
            break;
        }
    }
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
    fn spawn(mut actor: A, capacity: usize) -> Self {
        let (requests, messages) = tokio::sync::mpsc::channel(capacity);
        let addr = Addr { requests };
        tokio::spawn({
            let ctx_addr = addr.clone();
            async move {
                let mut ctx = Ctx {addr: &ctx_addr, should_stop: false};
                actor.starting(&ctx).await;
                handle_messages(&mut actor, &mut ctx, messages).await;
                actor.stopping(&ctx).await;
            }
        });
        addr
    }
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
        type Message = i32;
        
        fn handle(&mut self, ctx: &mut Ctx<'_, Self>, m: Self::Message) -> impl Future<Output = ()> + Send {
            async move {
                println!("{m}");
                self.stop(ctx).await;
            }
        }
        
        fn starting(&mut self, _ctx: &Ctx<'_, Self>) -> impl Future<Output = ()> + Send {
            async {}
        }
        
        fn stopping(&mut self, _ctx: &Ctx<'_, Self>) -> impl Future<Output = ()> + Send {
            async {
                println!("stopping");
            }
        }
    }
    #[tokio::test]
    async fn test() {
        let actor = TestActor;
        let addr = actor.start();
        addr.add_stream(futures_util::stream::iter(0 .. 123), |m| crate::actor::StreamItem::Next(m));
        drop(addr);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}