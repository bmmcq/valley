use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures::{ready, Sink};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::PollSender;
use crate::{ServerId, VError};

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

    pub fn try_send(&self, target: ServerId, data: T) -> Result<Option<T>, VError> {
        if let Some(send) = self.sends.get(&target) {
            match send.try_send(Some(data)) {
                Ok(_) => Ok(None),
                Err(TrySendError::Full(d)) => {
                    Ok(Some(d.unwrap()))
                },
                Err(TrySendError::Closed(_)) => {
                    Err(VError::SendError("closed".to_owned()))
                }
            }
        } else {
            Err(VError::ServerNotFound(target))
        }
    }

    pub fn try_flush(&self) -> Result<(), VError> {
        for se in self.sends.values() {
            match se.try_send(None) {
                Err(TrySendError::Closed(_)) => {
                    return Err(VError::SendError("closed".to_owned()));
                }
                _ => (),
            }
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


