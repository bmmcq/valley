use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncWrite};
use async_trait::async_trait;
use crate::{ChannelId, ServerId, VError};

#[async_trait]
pub trait ConnectionBuilder {
    type Reader: AsyncRead + Send + Unpin + 'static;
    type Writer: AsyncWrite + Send + Unpin + 'static;

    async fn bind(&mut self, addr: SocketAddr) -> Result<SocketAddr, VError>;

    async fn get_writer_to(&self, ch_id: ChannelId, target: ServerId, addr: SocketAddr) -> Result<Self::Writer, VError>;

    async fn get_reader_from(&self, ch_id: ChannelId, source: ServerId) -> Result<Self::Reader, VError>;
}

pub mod tcp;
