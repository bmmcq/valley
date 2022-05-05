use std::collections::HashMap;
use std::io::IoSlice;
use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::BytesMut;
use log::error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{ChannelId, Message, RemoteReceiver, RemoteSender, ServerId, VError};

#[async_trait]
pub trait NameService {
    async fn register(&self, server_id: ServerId, addr: SocketAddr) -> Result<(), VError>;

    async fn get_registered(&self, server_id: ServerId) -> Result<Option<SocketAddr>, VError>;
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
        loop {
            match reader.read_u64().await {
                Ok(len) => {
                    if len > 0 {
                        let size = len as usize;
                        buf.reserve(size);
                        buf.resize(size, 0);
                        let mut sp_buf = buf.split();
                        let bytes = sp_buf.as_mut();
                        match reader.read_exact(&mut bytes[..]).await {
                            Ok(r) => {
                                assert_eq!(r, size, "read_exact expect read {} actually read {};", size, r);
                            },
                            Err(e) => {
                                error!("fail to read message from {}: {}", source, e);
                                break;
                            }
                        }
                        if let Err(e) = input.send(Message::new(source, sp_buf.freeze())).await {
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

pub mod tcp;