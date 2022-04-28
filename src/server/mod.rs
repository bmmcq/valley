use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex};

use crate::{ChannelId, ServerId, VError};

pub trait NameService {}

enum Accepted<T> {
    NoWaiting(T),
    Waiting(oneshot::Sender<T>),
}

#[derive(Clone)]
struct WaitingAccepted<T> {
    waiting: Arc<Mutex<HashMap<(ServerId, ChannelId), Accepted<T>>>>,
}

impl<T> WaitingAccepted<T> {
    fn new() -> Self {
        WaitingAccepted { waiting: Arc::new(Mutex::new(HashMap::new())) }
    }

    async fn get_or_wait(&self, ch_id: ChannelId, server_id: ServerId) -> Result<T, VError> {
        let mut waiting = self.waiting.lock().await;
        if let Some(ac) = waiting.remove(&(server_id, ch_id)) {
            match ac {
                Accepted::NoWaiting(a) => Ok(a),
                Accepted::Waiting(_) => {
                    panic!("some other waiting on it;")
                }
            }
        } else {
            let (tx, rx) = oneshot::channel();
            waiting.insert((server_id, ch_id), Accepted::Waiting(tx));
            std::mem::drop(waiting);
            Ok(rx.await.unwrap())
        }
    }

    async fn notify(&self, ch_id: ChannelId, server_id: ServerId, res: T) -> Result<(), VError> {
        let mut waiting = self.waiting.lock().await;
        if let Some(ac) = waiting.remove(&(server_id, ch_id)) {
            match ac {
                Accepted::NoWaiting(_) => {
                    panic!("no waiting...")
                }
                Accepted::Waiting(notify) => {
                    notify
                        .send(res)
                        .map_err(|_| VError::SendError("notify fail".to_owned()))?;
                }
            }
        } else {
            waiting.insert((server_id, ch_id), Accepted::NoWaiting(res));
        }
        Ok(())
    }
}

pub struct TcpServer<N> {
    server_id: ServerId,
    addr: SocketAddr,
    name_service: N,
    waiting_accept: WaitingAccepted<TcpStream>,
}
