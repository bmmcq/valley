use std::collections::HashMap;
use std::net::SocketAddr;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::Sender;

use crate::codec::Encode;
use crate::connection::quic::QUIConnBuilder;
use crate::connection::tcp::TcpConnBuilder;
use crate::connection::ConnectionBuilder;
use crate::errors::{ConnectError, ServerError};
use crate::name_service::NameService;
use crate::receive::{EnumReceiver, VReceiver};
use crate::send::bound::VSender;
use crate::send::unbound::VUnboundSender;
use crate::{ChannelId, Message, ServerId, VError};

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
    ValleyServer::new(server_id, addr, name_service, TcpConnBuilder::new(server_id))
}

pub fn new_quic_server<N>(server_id: ServerId, addr: SocketAddr, name_service: N) -> ValleyServer<N, QUIConnBuilder> {
    ValleyServer::new(server_id, addr, name_service, QUIConnBuilder::new(server_id))
}

impl<N, B> ValleyServer<N, B>
where
    N: NameService,
    B: ConnectionBuilder,
{
    pub async fn start(&mut self) -> Result<(), ServerError> {
        let bind_addr = self.conn_builder.bind(self.addr).await?;
        self.name_service.register(self.server_id, bind_addr).await?;
        info!("server {} started at {} ;", self.server_id, bind_addr);
        self.addr = bind_addr;
        Ok(())
    }

    pub fn get_server_id(&self) -> ServerId {
        self.server_id
    }

    pub fn get_address(&self) -> SocketAddr {
        self.addr
    }

    /// Allocate a channel for bidirectional symmetric communication;
    /// The 'Symmetric' means it's not a client/server architecture(asymmetrical), instead, each role
    /// in the communication network acts like a server;
    /// Each server will send messages to other servers without waiting for responses, and will receive
    /// messages from other servers without sending responses;
    pub async fn alloc_symmetry_channel<T: Encode + Send + 'static>(
        &self, ch_id: ChannelId, servers: &[ServerId],
    ) -> Result<(VSender<T>, VReceiver), VError> {
        let mut addrs = Vec::with_capacity(servers.len());

        for server_id in servers {
            match self.name_service.get_registered(*server_id).await {
                Ok(Some(addr)) => {
                    addrs.push((*server_id, addr));
                }
                Ok(None) => {
                    return Err(ConnectError::ServerNotFound(*server_id))?;
                }
                Err(source) => {
                    return Err(ConnectError::FetchServerAddr { server_id: *server_id, source })?;
                }
            }
        }

        let mut sends = HashMap::with_capacity(servers.len());
        for (server_id, addr) in addrs {
            debug!("channel[{}]: try to connect server {} at  {} ...", ch_id, server_id, addr);
            let conn = self
                .conn_builder
                .get_writer_to(ch_id, self.server_id, addr)
                .await?;
            let (tx, rx) = tokio::sync::mpsc::channel(1024);
            start_send(ch_id, server_id, EnumReceiver::Bound(rx), conn);
            sends.insert(server_id, tx);
        }

        let (tx, rx) = tokio::sync::mpsc::channel(1024 * servers.len());
        for server_id in servers {
            let conn = self.conn_builder.get_reader_from(ch_id, *server_id).await?;
            start_recv(ch_id, *server_id, tx.clone(), conn);
        }

        Ok((VSender::new(self.server_id, sends), VReceiver::new(self.server_id, rx)))
    }

    pub async fn alloc_symmetry_channel_unbound_send<T: Encode + Send + 'static>(
        &self, ch_id: ChannelId, servers: &[ServerId],
    ) -> Result<(VUnboundSender<T>, VReceiver), VError> {
        let mut addrs = Vec::with_capacity(servers.len());

        for server_id in servers {
            match self.name_service.get_registered(*server_id).await {
                Ok(Some(addr)) => {
                    addrs.push((*server_id, addr));
                }
                Ok(None) => {
                    return Err(ConnectError::ServerNotFound(*server_id))?;
                }
                Err(source) => {
                    return Err(ConnectError::FetchServerAddr { server_id: *server_id, source })?;
                }
            }
        }

        let mut sends = HashMap::with_capacity(servers.len());
        for (server_id, addr) in addrs {
            debug!("channel[{}]: try to connect server {} at  {} ...", ch_id, server_id, addr);
            let conn = self
                .conn_builder
                .get_writer_to(ch_id, self.server_id, addr)
                .await?;
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            start_send(ch_id, server_id, EnumReceiver::Unbound(rx), conn);
            sends.insert(server_id, tx);
        }

        let (tx, rx) = tokio::sync::mpsc::channel(1024 * servers.len());
        for server_id in servers {
            let conn = self.conn_builder.get_reader_from(ch_id, *server_id).await?;
            start_recv(ch_id, *server_id, tx.clone(), conn);
        }

        Ok((VUnboundSender::new(self.server_id, sends), VReceiver::new(self.server_id, rx)))
    }
}

struct WriteBufSlab {
    slab: BytesMut,
}

impl WriteBufSlab {
    fn new() -> Self {
        Self { slab: BytesMut::with_capacity(1 << 16) }
    }

    #[inline]
    fn write<T: Encode>(&mut self, entry: T) -> usize {
        let start_offset = self.slab.len();
        self.slab.put_u64(0);
        entry.write_to(&mut self.slab);

        let end_offset = self.slab.len();
        let payload_len = end_offset - start_offset - 8;
        if payload_len > 0 {
            let mut reset = &mut self.slab.as_mut()[start_offset..];
            reset.put_u64(payload_len as u64);
        } else {
            unsafe { self.slab.set_len(start_offset) }
        }
        payload_len
    }

    #[inline]
    fn len(&self) -> usize {
        self.slab.len()
    }

    #[inline]
    fn take_buf(&mut self) -> Option<Bytes> {
        if self.slab.is_empty() {
            None
        } else {
            Some(self.slab.split().freeze())
        }
    }
}

fn start_send<T, W>(ch_id: ChannelId, target: ServerId, mut output: EnumReceiver<Option<T>>, mut writer: W)
where
    T: Encode + Send + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let mut slab = WriteBufSlab::new();
    tokio::spawn(async move {
        debug!("channel[{}]: start to send message to server {};", ch_id, target);
        let mut error_occurred = false;
        'main: while let Some(mut next) = output.recv().await {
            if let Some(msg) = next.take() {
                slab.write(msg);
                'sub: loop {
                    match output.try_recv() {
                        Ok(Some(msg)) => {
                            slab.write(msg);
                            // todo: auto flush to avoid large buffer incurred by busy writes;
                        }
                        Ok(None) => {
                            if let Err(err) = send_flush(&mut slab, &mut writer).await {
                                error_occurred = true;
                                error!("channel[{}]: fail to send or flush: {};", ch_id, err);
                                break 'main;
                            }
                        }
                        Err(TryRecvError::Empty) => {
                            break 'sub;
                        }
                        Err(TryRecvError::Disconnected) => {
                            break 'main;
                        }
                    }
                }
            }
            if let Err(err) = send_flush(&mut slab, &mut writer).await {
                error_occurred = true;
                error!("channel[{}]: fail to send or flush: {};", ch_id, err);
                break;
            }
        }

        if !error_occurred && slab.len() > 0 {
            if let Err(err) = send_flush(&mut slab, &mut writer).await {
                error!("channel[{}]: fail to send or flush: {};", ch_id, err);
            }
        }

        debug!("channel[{}] :finish send message to server {};", ch_id, target);
        if let Err(err) = writer.shutdown().await {
            error!("channel[{}]: fail to shutdown: {};", ch_id, err);
        }
    });
}

#[inline]
async fn send_flush<W>(slab: &mut WriteBufSlab, writer: &mut W) -> std::io::Result<()>
where
    W: AsyncWrite + Send + Unpin + 'static,
{
    if let Some(mut buf) = slab.take_buf() {
        writer.write_all_buf(&mut buf).await?;
        writer.flush().await?;
    }
    Ok(())
}

struct ReadBufSlab {
    slab: BytesMut,
    waiting: usize,
}

impl ReadBufSlab {
    fn new() -> Self {
        Self { slab: BytesMut::with_capacity(1 << 16), waiting: 0 }
    }

    #[inline]
    fn get_read_buf(&mut self) -> &mut BytesMut {
        self.slab.reserve(1);
        &mut self.slab
    }

    #[inline]
    fn extract(&mut self) -> Option<Bytes> {
        if self.waiting > 0 {
            if self.slab.len() >= self.waiting {
                let bytes = self.slab.split_to(self.waiting).freeze();
                self.waiting = 0;
                Some(bytes)
            } else {
                None
            }
        } else if self.slab.len() > 8 {
            let new_size = self.slab.get_u64() as usize;
            if self.slab.len() >= new_size {
                let bytes = self.slab.split_to(new_size).freeze();
                Some(bytes)
            } else {
                self.waiting = new_size;
                None
            }
        } else {
            None
        }
    }
}

fn start_recv<R>(ch_id: ChannelId, source: ServerId, input: Sender<Message>, mut reader: R)
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let mut slab = ReadBufSlab::new();
    tokio::spawn(async move {
        let mut cnt = 0;
        loop {
            match reader.read_buf(slab.get_read_buf()).await {
                Ok(len) => {
                    if len > 0 {
                        debug!("channel[{}]:read {} bytes from {}", ch_id, len, source);
                        while let Some(buf) = slab.extract() {
                            cnt += 1;
                            let msg = Message::new(source, buf);
                            if let Err(err) = input.send(msg).await {
                                error!("channel[{}]: fail to delivery message from server {}: {};", ch_id, source, err);
                            }
                        }
                    } else {
                        error!("channel[{}] :no bytes available, maybe EOF;", ch_id);
                        break;
                    }
                }
                Err(e) => {
                    error!("channel[{}]: fail to read from {}: {};", ch_id, source, e);
                    break;
                }
            }
        }
        debug!("channel[{}]: finish read all and exit, total read {} messages;", ch_id, cnt);
    });
}
