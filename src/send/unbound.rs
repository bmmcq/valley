use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;
use crate::{ServerId, VError};

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

    pub  fn send(&self, target: ServerId, data: T) -> Result<(), VError> {
        if let Some(send) = self.sends.get(&target) {
            Ok(send.send(Some(data))?)
        } else {
            Err(VError::ServerNotFound(target))
        }
    }

    pub async fn flush(&self) -> Result<(), VError> {
        for se in self.sends.values() {
            se.send(None)?;
        }
        Ok(())
    }
}



