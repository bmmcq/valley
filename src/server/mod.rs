use std::collections::HashMap;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::future::err;
use log::error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, Mutex};

use crate::{ChannelId, Message, RemoteReceiver, RemoteSender, ServerId, VError};

#[async_trait]
pub trait NameService {
    async fn register(&self, server_id: ServerId, addr: SocketAddr) -> Result<(), VError>;

    async fn get_registered(&self, server_id: ServerId) -> Result<Option<SocketAddr>, VError>;
}

enum MaybeFuture<T> {
    Ready(T),
    Waiting(oneshot::Sender<T>),
}

#[derive(Clone)]
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

#[async_trait]
pub trait ConnectionBuilder {
    type Reader: AsyncRead + Send + Unpin + 'static;
    type Writer: AsyncWrite + Send + Unpin + 'static;

    async fn bind(&mut self, addr: SocketAddr) -> Result<SocketAddr, VError>;

    async fn get_writer_to(&self, ch_id: ChannelId, target: ServerId, addr: SocketAddr) -> Result<Self::Writer, VError>;

    async fn get_reader_from(&self, ch_id: ChannelId, source: ServerId) -> Result<Self::Reader, VError>;
}

pub struct ValleyServer<N, B> {
    server_id: ServerId,
    addr: SocketAddr,
    name_service: N,
    conn_builder: B,
}

impl<N, B> ValleyServer<N, B> {
    pub fn new(server_id: ServerId, addr: SocketAddr, name_service: N, conn_builder: B) -> Self {
        Self { server_id, addr, name_service, conn_builder }
    }
}

impl<N, B> ValleyServer<N, B>
where
    N: NameService,
    B: ConnectionBuilder,
{
    pub async fn start(&mut self) -> Result<(), VError> {
        let bind_addr = self.conn_builder.bind(self.addr).await?;
        self.name_service.register(self.server_id, bind_addr).await?;
        Ok(())
    }

    pub async fn get_connections(
        &self, ch_id: ChannelId, servers: &[ServerId],
    ) -> Result<(RemoteSender, RemoteReceiver), VError> {
        let mut addrs = Vec::with_capacity(servers.len());

        for server_id in servers {
            if let Some(addr) = self.name_service.get_registered(*server_id).await? {
                addrs.push((*server_id, addr));
            } else {
                return Err(VError::ServerNotFound(*server_id));
            }
        }

        let mut sends = HashMap::with_capacity(servers.len());
        for (server_id, addr) in addrs {
            let conn = self.conn_builder.get_writer_to(ch_id, server_id, addr).await?;
            let (tx, rx) = tokio::sync::mpsc::channel(1024);
            start_send(server_id, rx, conn);
            sends.insert(server_id, tx);
        }

        let (tx, rx) = tokio::sync::mpsc::channel(1024 * servers.len());
        for server_id in servers {
            let conn = self.conn_builder.get_reader_from(ch_id, *server_id).await?;
            start_recv(*server_id, tx.clone(), conn);
        }

        Ok((RemoteSender::new(self.server_id, sends), RemoteReceiver::new(self.server_id, rx)))
    }
}

fn start_send<W>(target: ServerId, mut output: Receiver<Message>, mut writer: W)
where
    W: AsyncWrite + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        while let Some(next) = output.recv().await {
            let len = next.payload.len() as u64;
            let head = len.to_be_bytes();
            let ivc1 = IoSlice::new(&head[..]);
            let ivc2 = IoSlice::new(next.payload.as_ref());
            match writer.write_vectored(&[ivc1, ivc2]).await {
                Ok(_size) => {}
                Err(err) => {
                    error!("fail to send message to {}: {}", target, err);
                    break;
                }
            }
        }
    });
}

fn start_recv<R>(source: ServerId, input: Sender<Message>, mut reader: R)
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let mut buf = BytesMut::with_capacity(1 << 16);
    tokio::spawn(async move {
        'out: loop {
            match reader.read_u64().await {
                Ok(len) => {
                    if len > 0 {
                        buf.reserve(len as usize);
                        while buf.len() < len as usize {
                            match reader.read_buf(&mut buf).await {
                                Ok(_r) => {}
                                Err(err) => {
                                    error!("fail to read message from {}: {}", source, err);
                                    break 'out;
                                }
                            }
                        }

                        let bytes = buf.split_to(len as usize);
                        if let Err(e) = input.send(Message::new(source, bytes.freeze())).await {
                            error!("fail to delivery message from {} : {}", source, e);
                        }
                    } else {
                        // ignore
                    }
                }
                Err(e) => {
                    error!("fail to read bytes from {}: {}", source, e);
                    break;
                }
            }
        }
    });
}

mod tcp;