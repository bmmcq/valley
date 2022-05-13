use std::collections::HashMap;
use std::net::SocketAddr;

use bytes::{Buf, BufMut, BytesMut};
use log::{debug, error, info, warn};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::codec::Encode;
use crate::connection::tcp::TcpConnBuilder;
use crate::connection::ConnectionBuilder;
use crate::name_service::NameService;
use crate::{ChannelId, Message, RemoteReceiver, RemoteSender, ServerId, VError};

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

pub fn new_tcp_server<N>(server_id: ServerId, addr: SocketAddr, name_service: N) -> ValleyServer<N, TcpConnBuilder> {
    ValleyServer::new(server_id, addr, name_service, TcpConnBuilder::new())
}

impl<N, B> ValleyServer<N, B>
where
    N: NameService,
    B: ConnectionBuilder,
{
    pub async fn start(&mut self) -> Result<(), VError> {
        let bind_addr = self.conn_builder.bind(self.addr).await?;
        self.name_service.register(self.server_id, bind_addr).await?;
        info!("server {} started at {} ;", self.server_id, bind_addr);
        Ok(())
    }

    pub async fn get_connections<T: Encode + Send + 'static>(
        &self, ch_id: ChannelId, servers: &[ServerId],
    ) -> Result<(RemoteSender<T>, RemoteReceiver), VError> {
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
            debug!("try to connect server {} at  {} ...", server_id, addr);
            let conn = self.conn_builder.get_writer_to(ch_id, self.server_id, addr).await?;
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

const MAX_BUF_SIZE: usize = 1460;

fn start_send<T, W>(target: ServerId, mut output: Receiver<Option<T>>, mut writer: W)
where
    T: Encode + Send + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let mut slab = BytesMut::with_capacity(1 << 32);
    tokio::spawn(async move {
        debug!("start to send message to server {}", target);
        while let Some(mut next) = output.recv().await {
            if let Some(msg) = next.take() {
                let start = slab.len();
                slab.put_u64(0);
                if let Err(e) = msg.write_to(&mut slab) {
                    error!("fail to encode message: {}", e);
                    break;
                }
                let end = slab.len();
                let p_size = (end - start - 8) as u64;
                if p_size > 0 {
                    let mut reset = &mut slab.as_mut()[start..];
                    reset.put_u64(p_size);
                } else {
                    warn!("write empty message;")
                }

                if end >= MAX_BUF_SIZE {
                    //debug!("try to flush ...");
                    let mut buf = slab.split().freeze();
                    if let Err(err) = writer.write_all_buf(&mut buf).await {
                        error!("fail to send message to {}: {}", target, err);
                        break;
                    }

                    if let Err(err) = writer.flush().await {
                        error!("fail to flush message to {}: {}", target, err);
                        break;
                    }
                }
            } else {
                let mut buf = slab.split().freeze();
                if let Err(err) = writer.write_all_buf(&mut buf).await {
                    error!("fail to send message to {}: {}", target, err);
                    break;
                }

                if let Err(err) = writer.flush().await {
                    error!("fail to flush message to {}: {}", target, err);
                    break;
                }
            }
        }
        debug!("finish send message to server {}", target);
    });
}

fn start_recv<R>(source: ServerId, input: Sender<Message>, mut reader: R)
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let mut buf = BytesMut::with_capacity(1 << 16);
    let mut unfinished = 0;
    tokio::spawn(async move {
        loop {
            buf.reserve(1);
            match reader.read_buf(&mut buf).await {
                Ok(len) => {
                    if len > 0 {
                        debug!("read {} bytes from {}", len, source);
                        parse_load(source, &mut unfinished, &mut buf, &input).await;
                    } else {
                        error!("no bytes read, maybe EOF;");
                        break;
                    }
                }
                Err(e) => {
                    error!("fail to read from {}: {}", source, e);
                    break;
                }
            }
        }
    });
}

#[inline]
async fn parse_load(source: ServerId, unfinished: &mut usize, buf: &mut BytesMut, sink: &Sender<Message>) {
    while *unfinished < buf.len() {
        if *unfinished > 0 {
            let payload = buf.split_to(*unfinished);
            *unfinished = 0;
            if let Err(e) = sink.send(Message::new(source, payload.freeze())).await {
                error!("fail to delivery message from {} : {}", source, e);
            }
        } else {
            if buf.len() >= 8 {
                let new_size = buf.get_u64() as usize;
                if buf.len() >= new_size {
                    let payload = buf.split_to(new_size);
                    if let Err(e) = sink.send(Message::new(source, payload.freeze())).await {
                        error!("fail to delivery message from {}: {}", source, e);
                    }
                } else {
                    *unfinished = new_size;
                }
            } else {
                break;
            }
        }
    }
}
