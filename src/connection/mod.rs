use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::sync::{oneshot, Mutex};

use crate::errors::{ConnectError, ServerError};
use crate::{ChannelId, ServerId};

#[async_trait]
pub trait ConnectionBuilder {
    type Reader: AsyncRead + Send + Unpin + 'static;
    type Writer: AsyncWrite + Send + Unpin + 'static;

    async fn bind(&mut self, addr: SocketAddr) -> Result<SocketAddr, ServerError>;

    async fn get_writer_to(&self, ch_id: ChannelId, target: ServerId, addr: SocketAddr) -> Result<Self::Writer, ConnectError>;

    async fn get_reader_from(&self, ch_id: ChannelId, source: ServerId) -> Result<Self::Reader, ConnectError>;
}

enum MaybeFuture<T> {
    Ready(T),
    Waiting(oneshot::Sender<T>),
}

struct WaitingAccepted<T> {
    waiting: Arc<Mutex<HashMap<(ServerId, ChannelId), MaybeFuture<T>>>>,
}

impl<T> WaitingAccepted<T> {
    fn new() -> Self {
        WaitingAccepted { waiting: Arc::new(Mutex::new(HashMap::new())) }
    }

    async fn get_or_wait(&self, ch_id: ChannelId, server_id: ServerId) -> T {
        let mut waiting = self.waiting.lock().await;
        if let Some(ac) = waiting.remove(&(server_id, ch_id)) {
            match ac {
                MaybeFuture::Ready(a) => a,
                MaybeFuture::Waiting(_) => {
                    panic!("conflict channel id = {}", ch_id);
                }
            }
        } else {
            let (tx, rx) = oneshot::channel();
            waiting.insert((server_id, ch_id), MaybeFuture::Waiting(tx));
            debug!("wait connection of channel[{}] from {} ...;", ch_id, server_id);
            drop(waiting);
            rx.await.expect("wait connection fail;")
        }
    }

    async fn notify(&self, ch_id: ChannelId, server_id: ServerId, res: T) {
        let mut waiting = self.waiting.lock().await;
        if let Some(ac) = waiting.remove(&(server_id, ch_id)) {
            match ac {
                MaybeFuture::Ready(_) => {
                    panic!("unknown connection of channel {}", ch_id);
                }
                MaybeFuture::Waiting(notify) => {
                    debug!("notify waiting connection of channel[{}] from server {};", ch_id, server_id);
                    notify.send(res).ok();
                }
            }
        } else {
            waiting.insert((server_id, ch_id), MaybeFuture::Ready(res));
        }
    }
}

impl<T> Clone for WaitingAccepted<T> {
    fn clone(&self) -> Self {
        Self { waiting: self.waiting.clone() }
    }
}

#[inline]
fn add_connection<R>(mut conn: R, accepted: WaitingAccepted<R>)
where
    R: AsyncRead + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        let mut buf = [0u8; 8];
        match conn.read_exact(&mut buf[..]).await {
            Ok(n) => {
                assert_eq!(n, buf.len(), "read_exact fail expect read {}, actually read {};", buf.len(), n);
                let mut read = &buf[..];
                let server_id = read.get_u32();
                let ch_id = read.get_u32();
                debug!("get connection of channel[{}] from server {} ;", ch_id, server_id);
                accepted.notify(ch_id, server_id, conn).await;
            }
            Err(e) => {
                error!("read connection info fail: {}", e)
            }
        }
    });
}

#[inline]
fn get_head(ch_id: ChannelId, server_id: ServerId) -> [u8; 8] {
    let mut buf = [0u8; 8];
    let mut write = &mut buf[..];
    write.put_u32(server_id);
    write.put_u32(ch_id);
    buf
}

pub mod quic;
pub mod tcp;
