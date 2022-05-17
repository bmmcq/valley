#[macro_use]
extern crate log;

use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::mpsc::{Receiver, Sender};

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

pub struct RemoteSender<T> {
    server_id: ServerId,
    sends: HashMap<ServerId, Sender<Option<T>>>,
}

impl<T> RemoteSender<T> {
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

impl<T> Clone for RemoteSender<T> {
    fn clone(&self) -> Self {
        let mut sends_copy = HashMap::with_capacity(self.sends.len());
        for (k, v) in self.sends.iter() {
            sends_copy.insert(*k, v.clone());
        }
        Self { server_id: self.server_id, sends: sends_copy }
    }
}

pub struct RemoteReceiver {
    server_id: ServerId,
    receiver: Receiver<Message>,
}

impl RemoteReceiver {
    pub fn new(server_id: ServerId, receiver: Receiver<Message>) -> Self {
        Self { server_id, receiver }
    }

    pub fn get_server_id(&self) -> ServerId {
        self.server_id
    }

    pub async fn recv(&mut self) -> Option<Message> {
        self.receiver.recv().await
    }
}

/// Create a communication channel, with which we can send data to remote servers, and receive data from them;
pub async fn allocate<T>(_ch_id: ChannelId, _servers: &[ServerId]) -> Result<(RemoteSender<T>, RemoteReceiver), VError> {
    todo!()
}

pub mod codec;
pub mod connection;
pub mod errors;
pub mod name_service;
pub mod server;
