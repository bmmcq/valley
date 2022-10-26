

pub use connect::ConnectError;
pub use send::SendError;
pub use server::ServerError;
use thiserror::Error;

mod server;
mod connect;
mod send;

#[derive(Error, Debug)]
pub enum VError {
    #[error("server start error: {source};")]
    ServerStart {
        #[from]
        source: ServerError,
    },
    #[error("connect error: {source};")]
    Connect {
        #[from]
        source: ConnectError,
    },
    #[error("unknown error : {source:?}")]
    Unknown {
        #[from]
        source: anyhow::Error
    }
}
