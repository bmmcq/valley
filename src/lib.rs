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

    pub fn get_payload(&self) -> &[u8] {
        self.payload.as_ref()
    }
}

pub struct RemoteSender {
    server_id: ServerId,
    sends: HashMap<ServerId, Sender<Message>>,
}

impl RemoteSender {
    pub fn new(server_id: ServerId, sends: HashMap<ServerId, Sender<Message>>) -> Self {
        Self { server_id, sends }
    }

    pub async fn send(&self, target: ServerId, data: Bytes) -> Result<(), VError> {
        if let Some(send) = self.sends.get(&target) {
            let msg = Message { source: self.server_id, payload: data };

            Ok(send.send(msg).await?)
        } else {
            Err(VError::ServerNotFound(target))
        }
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
pub async fn allocate(_ch_id: ChannelId, _servers: &[ServerId]) -> Result<(RemoteSender, RemoteReceiver), VError> {
    todo!()
}

pub mod errors;
pub mod server;
