use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::net::TcpStream;
use tokio::sync::RwLock;

use crate::ServerId;

#[async_trait]
pub trait NameService {
    async fn register(&self, server_id: ServerId, addr: SocketAddr) -> Result<(), anyhow::Error>;

    async fn get_registered(&self, server_id: ServerId) -> Result<Option<SocketAddr>, anyhow::Error>;
}

pub struct StaticNameService {
    hosts: Vec<(ServerId, SocketAddr)>,
    naming_map: Arc<RwLock<HashMap<ServerId, SocketAddr>>>,
}

impl StaticNameService {
    pub fn new(hosts: Vec<(ServerId, SocketAddr)>) -> Self {
        Self { hosts, naming_map: Arc::new(RwLock::new(HashMap::new())) }
    }
}

#[async_trait]
impl NameService for StaticNameService {
    async fn register(&self, server_id: ServerId, addr: SocketAddr) -> Result<(), anyhow::Error> {
        let mut w_lock = self.naming_map.write().await;
        w_lock.insert(server_id, addr);
        while w_lock.len() < self.hosts.len() {
            for (id, addr) in self.hosts.iter() {
                if *id != server_id && !w_lock.contains_key(id) {
                    match TcpStream::connect(*addr).await {
                        Ok(_) => {
                            info!("server_{}({}) is started...", id, addr);
                            w_lock.insert(*id, *addr);
                        }
                        Err(e) => {
                            warn!("try to connect server {}({}) fail: {};", id, addr, e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn get_registered(&self, server_id: ServerId) -> Result<Option<SocketAddr>, anyhow::Error> {
        let read_lock = self.naming_map.read().await;
        Ok(read_lock.get(&server_id).map(|addr| *addr))
    }
}
