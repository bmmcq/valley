use thiserror::Error;

use crate::ServerId;

#[derive(Error, Debug)]
pub enum SendError {
    #[error("send fail because channel disconnected;")]
    Disconnected,
    #[error("the server(id={0}) which want to send to can't found;")]
    ServerNotFound(ServerId),
}
