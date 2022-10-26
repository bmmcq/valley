use std::collections::HashMap;

use tokio::sync::mpsc::UnboundedSender;

use crate::errors::SendError;
use crate::ServerId;

pub struct VUnboundSender<T> {
    server_id: ServerId,
    sends: HashMap<ServerId, UnboundedSender<Option<T>>>,
}

impl<T> VUnboundSender<T> {
    pub fn new(server_id: ServerId, sends: HashMap<ServerId, UnboundedSender<Option<T>>>) -> Self {
        Self { server_id, sends }
    }

    pub fn get_server_id(&self) -> ServerId {
        self.server_id
    }

    pub fn remote_servers(&self) -> impl Iterator<Item = ServerId> + '_ {
        self.sends.keys().copied()
    }

    pub fn send(&self, target: ServerId, data: T) -> Result<(), SendError> {
        if let Some(send) = self.sends.get(&target) {
            if let Err(_) = send.send(Some(data)) {
                Err(SendError::Disconnected)
            } else {
                Ok(())
            }
        } else {
            Err(SendError::ServerNotFound(target))
        }
    }

    pub fn flush(&self) -> Result<(), SendError> {
        for se in self.sends.values() {
            if let Err(_e) = se.send(None) {
                return Err(SendError::Disconnected);
            }
        }
        Ok(())
    }

    pub fn split(self) -> Vec<VUnboundServerSender<T>> {
        let Self { server_id, sends } = self;
        let mut per_server_sends = Vec::with_capacity(sends.len());
        for (id, send) in sends {
            per_server_sends.push(VUnboundServerSender::new(server_id, id, send));
        }
        per_server_sends
    }
}

impl<T> Clone for VUnboundSender<T> {
    fn clone(&self) -> Self {
        let mut sends = HashMap::with_capacity(self.sends.len());
        for (k, v) in self.sends.iter() {
            sends.insert(*k, v.clone());
        }
        Self { server_id: self.server_id, sends }
    }
}

pub struct VUnboundServerSender<T> {
    source_server_id: ServerId,
    target_server_id: ServerId,
    send: UnboundedSender<Option<T>>,
}

impl<T> VUnboundServerSender<T> {
    fn new(src: ServerId, dst: ServerId, send: UnboundedSender<Option<T>>) -> Self {
        Self { source_server_id: src, target_server_id: dst, send }
    }

    pub fn get_source_server_id(&self) -> ServerId {
        self.source_server_id
    }

    pub fn get_target_server_id(&self) -> ServerId {
        self.target_server_id
    }

    pub fn send(&self, data: T) -> Result<(), SendError> {
        if let Err(_) = self.send.send(Some(data)) {
            Err(SendError::Disconnected)
        } else {
            Ok(())
        }
    }

    pub fn flush(&self) -> Result<(), SendError> {
        if let Err(_) = self.send.send(None) {
            Err(SendError::Disconnected)
        } else {
            Ok(())
        }
    }
}

impl<T> Clone for VUnboundServerSender<T> {
    fn clone(&self) -> Self {
        Self { source_server_id: self.source_server_id, target_server_id: self.target_server_id, send: self.send.clone() }
    }
}
