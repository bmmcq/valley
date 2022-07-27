use std::collections::HashMap;

use bytes::{Buf, BufMut};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};

use crate::codec::{Decode, Encode};
use crate::connection::tcp::TcpConnBuilder;
use crate::name_service::NameService;
use crate::server::ValleyServer;
use crate::{ChannelId, ServerId, VError};
use crate::receive::VReceiver;
use crate::send::bound::VSender;

struct MPacket<T> {
    ch_id: ChannelId,
    data: T,
}

impl<T: Encode> Encode for MPacket<T> {
    fn write_to<W: BufMut>(&self, writer: &mut W) {
        writer.put_u32(self.ch_id);
        self.data.write_to(writer);
    }
}

impl<T: Decode> Decode for MPacket<T> {
    fn read_from<R: Buf>(reader: &mut R) -> std::io::Result<Self> {
        let ch_id = reader.get_u32();
        let data = T::read_from(reader)?;
        Ok(Self { ch_id, data })
    }
}

pub struct MSender<T> {
    ch_id: ChannelId,
    inner: VSender<MPacket<T>>,
}

impl<T> MSender<T> {
    pub async fn send(&self, target: ServerId, data: T) -> Result<(), VError> {
        self.inner
            .send(target, MPacket { ch_id: self.ch_id, data })
            .await
    }

    pub async fn flush(&self) -> Result<(), VError> {
        self.inner.flush().await
    }
}

pub struct ValleyTcpMpServer<N, T> {
    server: ValleyServer<N, TcpConnBuilder>,
    all_servers: usize,
    tcp_connections: usize,
    send_pool: Vec<VSender<MPacket<T>>>,
    tx_register: Vec<UnboundedSender<(ChannelId, Sender<T>)>>,
}

impl<N, T> ValleyTcpMpServer<N, T>
where
    N: NameService,
    T: Encode + Decode + Send + 'static,
{
    pub async fn start(&mut self) -> Result<(), VError> {
        self.server.start().await?;
        let mut peers = Vec::with_capacity(self.all_servers - 1);
        for i in 0..self.all_servers {
            let peer_id = i as ServerId;
            if peer_id != self.server.server_id {
                peers.push(peer_id);
            }
        }

        let mut delivery_tasks = vec![];

        for i in 0..self.tcp_connections {
            let (tx, rx) = self
                .server
                .alloc_symmetry_channel(i as ChannelId, &peers)
                .await?;
            self.send_pool.push(tx);
            let (ttx, rtx) = tokio::sync::mpsc::unbounded_channel();
            self.tx_register.push(ttx);
            delivery_tasks.push(DeliveryTask::new(rtx, rx));
        }

        for task in delivery_tasks {
            task.run();
        }
        Ok(())
    }

    pub async fn get_bi_channel(&self, ch_id: ChannelId) -> Result<(MSender<T>, Receiver<T>), VError> {
        let stream_id = ch_id as usize % self.tcp_connections;
        if self.send_pool.len() <= stream_id || self.tx_register.len() <= stream_id {
            return Err(VError::ServerNotFound(self.server.server_id));
        }

        let inner = self.send_pool[stream_id].clone();
        let send = MSender { ch_id, inner };
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        if let Err(e) = self.tx_register[stream_id].send((ch_id, tx)) {
            error!("register receiver fail: {}", e);
            return Err(VError::ServerNotStart(self.server.server_id));
        }
        Ok((send, rx))
    }
}

struct DeliveryTask<T> {
    shutdown: bool,
    tx_register: UnboundedReceiver<(ChannelId, Sender<T>)>,
    tx_map: HashMap<ChannelId, Sender<T>>,
    recv: VReceiver,
}

impl<T> DeliveryTask<T> {
    fn new(tx_register: UnboundedReceiver<(ChannelId, Sender<T>)>, recv: VReceiver) -> Self {
        Self { shutdown: false, tx_register, tx_map: HashMap::new(), recv }
    }
}

impl<T: Encode + Decode + Send + 'static> DeliveryTask<T> {
    #[inline]
    async fn wait_register(
        ch_id: ChannelId, tx_register: &mut UnboundedReceiver<(ChannelId, Sender<T>)>,
        tx_map: &mut HashMap<ChannelId, Sender<T>>,
    ) -> bool {
        match tx_register.recv().await {
            Some((mut new_ch_id, tx)) => {
                tx_map.insert(new_ch_id, tx);
                while ch_id != new_ch_id {
                    match tx_register.try_recv() {
                        Ok((cid, tx)) => {
                            tx_map.insert(cid, tx);
                            new_ch_id = cid;
                        }
                        Err(TryRecvError::Empty) => {
                            return false;
                        }
                        Err(TryRecvError::Disconnected) => {
                            return true;
                        }
                    }
                }
                false
            }
            None => true,
        }
    }

    pub fn run(self) {
        let DeliveryTask { mut shutdown, mut tx_register, mut recv, mut tx_map } = self;
        tokio::spawn(async move {
            let mut block: Option<(ServerId, MPacket<T>)> = None;
            loop {
                if let Some((source, packet)) = block.take() {
                    if shutdown {
                        error!("discard data from server {} to channel {};", source, packet.ch_id);
                    } else {
                        let ch_id = packet.ch_id;
                        shutdown = Self::wait_register(ch_id, &mut tx_register, &mut tx_map).await;
                        if let Some(send) = tx_map.get(&ch_id) {
                            if let Err(e) = send.send(packet.data).await {
                                error!("fail to delivery data from server {} to channel {}: {}", source, ch_id, e);
                            }
                        } else {
                            block = Some((source, packet));
                        }
                    }
                } else {
                    match recv.recv().await {
                        Some(mut msg) => match MPacket::<T>::read_from(&mut msg.payload) {
                            Ok(packet) => {
                                let ch_id = packet.ch_id;
                                if let Some(send) = tx_map.get(&ch_id) {
                                    if let Err(e) = send.send(packet.data).await {
                                        error!("fail to delivery data from server {} to channel {}: {}'", msg.source, ch_id, e);
                                    }
                                } else {
                                    block = Some((msg.source, packet));
                                }
                            }
                            Err(e) => {
                                error!("can't decode data from server {}: {}", msg.source, e);
                            }
                        },
                        None => break,
                    }
                }
            }
        });
    }
}
