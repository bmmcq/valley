#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{ready, Sink, Stream};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::PollSender;

use crate::errors::VError;

pub type ServerId = u32;
pub type ChannelId = u32;

pub struct Message {
    source: ServerId,
    payload: Bytes,
}

impl Message {
    pub fn new(source: ServerId, payload: Bytes) -> Self {
        Message { source, payload }
    }

    pub fn get_source(&self) -> ServerId {
        self.source
    }

    pub fn get_payload(&mut self) -> &mut Bytes {
        &mut self.payload
    }
}

impl AsRef<[u8]> for Message {
    fn as_ref(&self) -> &[u8] {
        self.payload.as_ref()
    }
}

pub struct VSender<T> {
    server_id: ServerId,
    sends: HashMap<ServerId, Sender<Option<T>>>,
}

impl<T> VSender<T> {
    pub fn new(server_id: ServerId, sends: HashMap<ServerId, Sender<Option<T>>>) -> Self {
        Self { server_id, sends }
    }

    pub fn get_server_id(&self) -> ServerId {
        self.server_id
    }

    pub async fn send(&self, target: ServerId, data: T) -> Result<(), VError> {
        if let Some(send) = self.sends.get(&target) {
            Ok(send.send(Some(data)).await?)
        } else {
            Err(VError::ServerNotFound(target))
        }
    }

    pub async fn flush(&self) -> Result<(), VError> {
        for se in self.sends.values() {
            se.send(None).await?;
        }
        Ok(())
    }
}

impl<T> Clone for VSender<T> {
    fn clone(&self) -> Self {
        let mut sends_copy = HashMap::with_capacity(self.sends.len());
        for (k, v) in self.sends.iter() {
            sends_copy.insert(*k, v.clone());
        }
        Self { server_id: self.server_id, sends: sends_copy }
    }
}

pub struct VPollSender<T> {
    server_id: ServerId,
    sends: HashMap<ServerId, (PollSender<Option<T>>, bool)>,
}

impl<T: Send + 'static> VPollSender<T> {
    pub fn get_server_id(&self) -> ServerId {
        self.server_id
    }

    pub fn poll_send(
        self: Pin<&mut Self>, target: ServerId, item: &mut Option<T>, cx: &mut Context<'_>,
    ) -> Poll<Result<(), VError>> {
        if item.is_none() {
            return Poll::Ready(Ok(()));
        }

        let this = Pin::into_inner(self);
        if let Some((a, is_dirty)) = this.sends.get_mut(&target) {
            let mut pin = Pin::new(a);
            if let Err(_) = ready!(pin.as_mut().poll_ready(cx)) {
                return Poll::Ready(Err(VError::SendError("disconnected".to_owned())));
            }

            if let Some(item) = item.take() {
                match pin.start_send(Some(item)) {
                    Ok(_) => {
                        *is_dirty |= true;
                        Poll::Ready(Ok(()))
                    }
                    Err(_) => Poll::Ready(Err(VError::SendError("disconnected".to_owned()))),
                }
            } else {
                Poll::Ready(Ok(()))
            }
        } else {
            Poll::Ready(Err(VError::ServerNotFound(target)))
        }
    }

    pub fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), VError>> {
        let this = Pin::into_inner(self);
        for (_, (server, is_dirty)) in this.sends.iter_mut() {
            if *is_dirty {
                let mut pin = Pin::new(server);
                match ready!(pin.as_mut().poll_flush(cx)) {
                    Ok(_) => match pin.start_send(None) {
                        Ok(_) => {
                            *is_dirty = false;
                        }
                        Err(_) => {
                            return Poll::Ready(Err(VError::SendError("disconnected".to_owned())));
                        }
                    },
                    Err(_) => {
                        return Poll::Ready(Err(VError::SendError("disconnected".to_owned())));
                    }
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl<T: Send + 'static> From<VSender<T>> for VPollSender<T> {
    fn from(raw: VSender<T>) -> Self {
        let mut poll_sends = HashMap::with_capacity(raw.sends.len());
        let VSender { server_id, sends } = raw;
        for (id, send) in sends {
            poll_sends.insert(id, (PollSender::new(send), false));
        }
        VPollSender { server_id, sends: poll_sends }
    }
}

impl<T: Send + 'static> Clone for VPollSender<T> {
    fn clone(&self) -> Self {
        let mut sends_copy = HashMap::with_capacity(self.sends.len());
        for (k, v) in self.sends.iter() {
            sends_copy.insert(*k, (v.0.clone(), false));
        }
        Self { server_id: self.server_id, sends: sends_copy }
    }
}

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

/// Create a communication channel, with which we can send data to remote servers, and receive data from them;
pub async fn allocate<T>(_ch_id: ChannelId, _servers: &[ServerId]) -> Result<(VSender<T>, VReceiver), VError> {
    todo!()
}

pub mod codec;
pub mod connection;
pub mod errors;
pub mod name_service;
pub mod server;
