use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::{ServerId, VError};

#[async_trait]
pub trait NameService {
    async fn register(&self, server_id: ServerId, addr: SocketAddr) -> Result<(), VError>;

    async fn get_registered(&self, server_id: ServerId) -> Result<Option<SocketAddr>, VError>;
}

pub struct StaticNameService {
    naming_map: Arc<RwLock<HashMap<ServerId, SocketAddr>>>
}

impl StaticNameService {
    pub fn new() -> Self {
        Self { naming_map: Arc::new(RwLock::new(HashMap::new())) }
    }
}

#[async_trait]
impl NameService for StaticNameService {
    async fn register(&self, server_id: ServerId, addr: SocketAddr) -> Result<(), VError> {
        let mut write_lock = self.naming_map.write().await;
        write_lock.insert(server_id, addr);
        Ok(())
    }

    async fn get_registered(&self, server_id: ServerId) -> Result<Option<SocketAddr>, VError> {
        let read_lock = self.naming_map.read().await;
        Ok(read_lock.get(&server_id).map(|addr| *addr))
    }
}