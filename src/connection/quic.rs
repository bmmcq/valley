use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::StreamExt;
use quinn::{ClientConfig, Endpoint, NewConnection, RecvStream, SendStream, ServerConfig};
use rustls::client::ServerCertVerified;
use rustls::{Certificate, Error, ServerName};
use tokio::sync::RwLock;

use crate::connection::{add_connection, get_head, ConnectionBuilder, WaitingAccepted};
use crate::errors::{ConnectError, ServerError};
use crate::{ChannelId, ServerId};

const SERVER_NAME: &'static str = "valley";

pub struct QUIConnBuilder {
    server_id: ServerId,
    endpoint: Option<Endpoint>,
    accepted_recv: WaitingAccepted<RecvStream>,
    connected: RwLock<HashMap<ServerId, NewConnection>>,
}

impl QUIConnBuilder {
    pub fn new(server_id: ServerId) -> Self {
        Self { server_id, endpoint: None, accepted_recv: WaitingAccepted::new(), connected: RwLock::new(HashMap::new()) }
    }
}

#[async_trait]
impl ConnectionBuilder for QUIConnBuilder {
    type Reader = RecvStream;
    type Writer = SendStream;

    async fn bind(&mut self, addr: SocketAddr) -> Result<SocketAddr, ServerError> {
        let server_config = config_server();
        let (endpoint, mut incoming) = Endpoint::server(server_config, addr)?;
        let bind_addr = endpoint.local_addr()?;
        self.endpoint = Some(endpoint);
        let accepted_recv = self.accepted_recv.clone();
        tokio::spawn(async move {
            while let Some(next) = incoming.next().await {
                match next.await {
                    Ok(conn) => {
                        debug!("accept a new connection from {};", conn.connection.remote_address());
                        handle_new_connection(conn, &accepted_recv);
                    }
                    Err(err) => {
                        error!("get connection error: {}", err.to_string());
                    }
                }
            }
        });
        Ok(bind_addr)
    }

    async fn get_writer_to(&self, ch_id: ChannelId, target: ServerId, addr: SocketAddr) -> Result<Self::Writer, ConnectError> {
        {
            let read_lock = self.connected.read().await;
            if let Some(conn) = read_lock.get(&target) {
                return open_uni(ch_id, self.server_id, conn).await;
            }
        }

        let client_config = config_client();
        let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap())?;
        endpoint.set_default_client_config(client_config);
        let conn = endpoint.connect(addr, SERVER_NAME)?.await?;
        let result = open_uni(ch_id, self.server_id, &conn).await;
        let mut write_lock = self.connected.write().await;
        write_lock.insert(target, conn);
        result
    }

    async fn get_reader_from(&self, ch_id: ChannelId, source: ServerId) -> Result<Self::Reader, ConnectError> {
        let conn = self.accepted_recv.get_or_wait(ch_id, source).await;
        Ok(conn)
    }
}

#[inline]
fn handle_new_connection(mut conn: NewConnection, accepted_recv: &WaitingAccepted<RecvStream>) {
    let accepted_recv = accepted_recv.clone();
    tokio::spawn(async move {
        while let Some(next) = conn.uni_streams.next().await {
            match next {
                Ok(stream) => {
                    add_connection(stream, accepted_recv.clone());
                }
                Err(err) => {
                    error!("fail to get recv stream: {}", err.to_string());
                    break;
                }
            }
        }
    });
}

async fn open_uni(ch_id: ChannelId, server_id: ServerId, conn: &NewConnection) -> Result<SendStream, ConnectError> {
    match conn.connection.open_uni().await {
        Ok(mut stream) => {
            let head = get_head(ch_id, server_id);
            stream.write_all(&head[..]).await?;
            Ok(stream)
        }
        Err(err) => Err(err)?,
    }
}

fn config_server() -> ServerConfig {
    let cert = rcgen::generate_simple_self_signed(vec![SERVER_NAME.into()]).unwrap();
    let cert_der = cert.serialize_der().unwrap();
    let priv_key = rustls::PrivateKey(cert.serialize_private_key_der());
    let cert_chain = vec![rustls::Certificate(cert_der)];
    let mut server_config = ServerConfig::with_single_cert(cert_chain, priv_key).unwrap();
    Arc::get_mut(&mut server_config.transport)
        .unwrap()
        .max_concurrent_uni_streams(1024u32.into());

    server_config
}

struct SkipServerVerification;

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self, _: &Certificate, _: &[Certificate], _: &ServerName, _: &mut dyn Iterator<Item = &[u8]>, _: &[u8], _: SystemTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

fn config_client() -> ClientConfig {
    let builder = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    ClientConfig::new(Arc::new(builder))
}
