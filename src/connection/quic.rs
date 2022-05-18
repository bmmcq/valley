use std::net::SocketAddr;
use crate::connection::ConnectionBuilder;
use crate::{ChannelId, ServerId, VError};
use async_trait::async_trait;
use quinn::{RecvStream, SendStream};

pub struct QUIConnBuilder {
    server_id: ServerId
}

impl QUIConnBuilder {
    pub fn new(server_id: ServerId) -> Self {
        Self {
            server_id
        }
    }
}

#[async_trait]
impl ConnectionBuilder for QUIConnBuilder {
    type Reader = RecvStream;
    type Writer = SendStream;

    async fn bind(&mut self, _addr: SocketAddr) -> Result<SocketAddr, VError> {
        todo!()
    }

    async fn get_writer_to(&self, _ch_id: ChannelId, _target: ServerId, _addr: SocketAddr) -> Result<Self::Writer, VError> {
        todo!()
    }

    async fn get_reader_from(&self, _ch_id: ChannelId, _source: ServerId) -> Result<Self::Reader, VError> {
        todo!()
    }
}