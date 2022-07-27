#[macro_use]
extern crate log;


use bytes::Bytes;

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

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn get_payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn get_payload_mut(&mut self) -> &mut Bytes {
        &mut self.payload
    }

    pub fn  take_payload(self) -> Bytes {
        self.payload
    }
}

impl AsRef<[u8]> for Message {
    fn as_ref(&self) -> &[u8] {
        self.payload.as_ref()
    }
}

pub mod send;
pub mod receive;

pub mod codec;
pub mod connection;
pub mod errors;
pub mod name_service;
pub mod server;
