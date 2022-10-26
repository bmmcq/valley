use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};
use tokio_stream::wrappers::ReceiverStream;

use crate::{Message, ServerId};

pub struct VReceiver {
    server_id: ServerId,
    receiver: Receiver<Message>,
}

impl VReceiver {
    pub fn new(server_id: ServerId, receiver: Receiver<Message>) -> Self {
        Self { server_id, receiver }
    }

    pub fn get_server_id(&self) -> ServerId {
        self.server_id
    }
}

impl Deref for VReceiver {
    type Target = Receiver<Message>;

    fn deref(&self) -> &Self::Target {
        &self.receiver
    }
}

impl DerefMut for VReceiver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.receiver
    }
}

pub struct VReceiverStream {
    server_id: ServerId,
    rx_stream: ReceiverStream<Message>,
}

impl Deref for VReceiverStream {
    type Target = ReceiverStream<Message>;

    fn deref(&self) -> &Self::Target {
        &self.rx_stream
    }
}

impl DerefMut for VReceiverStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx_stream
    }
}

impl VReceiverStream {
    pub fn get_server_id(&self) -> ServerId {
        self.server_id
    }
}

impl Stream for VReceiverStream {
    type Item = Message;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().rx_stream).poll_next(cx)
    }
}

impl From<VReceiver> for VReceiverStream {
    fn from(raw: VReceiver) -> Self {
        let VReceiver { server_id, receiver } = raw;
        VReceiverStream { server_id, rx_stream: ReceiverStream::new(receiver) }
    }
}

pub enum EnumReceiver<T> {
    Bound(Receiver<T>),
    Unbound(UnboundedReceiver<T>),
}

impl<T> EnumReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        match self {
            EnumReceiver::Bound(s) => s.recv().await,
            EnumReceiver::Unbound(s) => s.recv().await,
        }
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        match self {
            EnumReceiver::Bound(s) => s.try_recv(),
            EnumReceiver::Unbound(s) => s.try_recv(),
        }
    }
}
