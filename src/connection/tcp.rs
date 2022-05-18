use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use crate::connection::{add_connection, get_head, ConnectionBuilder, WaitingAccepted};
use crate::{ChannelId, ServerId, VError};

pub struct TcpConnBuilder {
    server_id: ServerId,
    incoming: WaitingAccepted<TcpStream>,
}

impl TcpConnBuilder {
    pub fn new(server_id: ServerId) -> Self {
        Self { server_id, incoming: WaitingAccepted::new() }
    }
}

#[async_trait]
impl ConnectionBuilder for TcpConnBuilder {
    type Reader = TcpStream;
    type Writer = TcpStream;

    async fn bind(&mut self, addr: SocketAddr) -> Result<SocketAddr, VError> {
        let listener = TcpListener::bind(addr).await?;
        let bind_addr = listener.local_addr()?;
        let incoming = self.incoming.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((conn, addr)) => {
                        let accepted = incoming.clone();
                        debug!("accept a connection from {}", addr);
                        add_connection(conn, accepted);
                    }
                    Err(e) => {
                        error!("call 'accept()' fail: {}", e);
                        break;
                    }
                }
            }
        });
        Ok(bind_addr)
    }

    async fn get_writer_to(&self, ch_id: ChannelId, target: ServerId, addr: SocketAddr) -> Result<Self::Writer, VError> {
        let mut conn = TcpStream::connect(addr).await?;
        debug!("channel[{}]: create a TCP connection from server {} to {}({});", ch_id, self.server_id, target, addr);
        let head = get_head(ch_id, self.server_id);
        conn.write_all(&head[..]).await?;
        Ok(conn)
    }

    async fn get_reader_from(&self, ch_id: ChannelId, source: ServerId) -> Result<Self::Reader, VError> {
        let conn = self.incoming.get_or_wait(ch_id, source).await?;
        Ok(conn)
    }
}
