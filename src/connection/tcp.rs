use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Mutex};

use crate::connection::ConnectionBuilder;
use crate::{ChannelId, ServerId, VError};

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

    async fn get_or_wait(&self, ch_id: ChannelId, server_id: ServerId) -> Result<T, VError> {
        let mut waiting = self.waiting.lock().await;
        if let Some(ac) = waiting.remove(&(server_id, ch_id)) {
            match ac {
                MaybeFuture::Ready(a) => Ok(a),
                MaybeFuture::Waiting(_) => {
                    panic!("some other waiting on it;")
                }
            }
        } else {
            let (tx, rx) = oneshot::channel();
            waiting.insert((server_id, ch_id), MaybeFuture::Waiting(tx));
            debug!("wait connection of channel[{}] from {} ...;", ch_id, server_id);
            std::mem::drop(waiting);
            Ok(rx.await.unwrap())
        }
    }

    async fn notify(&self, ch_id: ChannelId, server_id: ServerId, res: T) -> Result<(), VError> {
        let mut waiting = self.waiting.lock().await;
        if let Some(ac) = waiting.remove(&(server_id, ch_id)) {
            match ac {
                MaybeFuture::Ready(_) => {
                    panic!("no waiting...")
                }
                MaybeFuture::Waiting(notify) => {
                    debug!("notify waiting connection of channel[{}] from server {};", ch_id, server_id);
                    notify
                        .send(res)
                        .map_err(|_| VError::SendError("notify fail".to_owned()))?;
                }
            }
        } else {
            waiting.insert((server_id, ch_id), MaybeFuture::Ready(res));
        }
        Ok(())
    }
}

impl<T> Clone for WaitingAccepted<T> {
    fn clone(&self) -> Self {
        Self { waiting: self.waiting.clone() }
    }
}

pub struct TcpConnBuilder {
    server_id: ServerId,
    incoming: WaitingAccepted<TcpStream>,
}

impl TcpConnBuilder {
    pub fn new(server_id: ServerId) -> Self {
        Self { server_id, incoming: WaitingAccepted::new() }
    }
}

#[async_trait]
impl ConnectionBuilder for TcpConnBuilder {
    type Reader = TcpStream;
    type Writer = TcpStream;

    async fn bind(&mut self, addr: SocketAddr) -> Result<SocketAddr, VError> {
        let listener = TcpListener::bind(addr).await?;
        let bind_addr = listener.local_addr()?;
        let incoming = self.incoming.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((conn, addr)) => {
                        let accepted = incoming.clone();
                        debug!("accept a connection from {}", addr);
                        add_connection(conn, accepted);
                    }
                    Err(e) => {
                        error!("call 'accept()' fail: {}", e);
                        break;
                    }
                }
            }
        });
        Ok(bind_addr)
    }

    async fn get_writer_to(&self, ch_id: ChannelId, target: ServerId, addr: SocketAddr) -> Result<Self::Writer, VError> {
        let mut buf = [0u8; 8];
        let mut write = &mut buf[..];
        write.put_u32(self.server_id);
        write.put_u32(ch_id);
        let mut conn = TcpStream::connect(addr).await?;
        debug!("channel[{}]: create a TCP connection from server {} to {}({});", ch_id, self.server_id, target, addr);
        conn.write_all(&buf[..]).await?;
        Ok(conn)
    }

    async fn get_reader_from(&self, ch_id: ChannelId, source: ServerId) -> Result<Self::Reader, VError> {
        let conn = self.incoming.get_or_wait(ch_id, source).await?;
        Ok(conn)
    }
}

#[inline]
fn add_connection(mut conn: TcpStream, accepted: WaitingAccepted<TcpStream>) {
    tokio::spawn(async move {
        let mut buf = [0u8; 8];
        match conn.read_exact(&mut buf[..]).await {
            Ok(n) => {
                assert_eq!(n, buf.len(), "read_exact fail expect read {}, actually read {};", buf.len(), n);
                let mut read = &buf[..];
                let server_id = read.get_u32();
                let ch_id = read.get_u32();
                debug!("get connection of channel[{}] from server {} ;", ch_id, server_id);
                if let Err(e) = accepted.notify(ch_id, server_id, conn).await {
                    error!("notify connection of channel {} from server {} fail: {};", ch_id, server_id, e);
                }
            }
            Err(e) => {
                error!("read connection info fail: {}", e)
            }
        }
    });
}
