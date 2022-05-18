use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use tokio::sync::mpsc::error::SendError;

use crate::ServerId;

#[derive(Debug)]
pub enum VError {
    ServerNotFound(ServerId),
    ServerNotStart(ServerId),
    SendError(String),
    StdIO(std::io::Error),
    WriteError(quinn::WriteError),
    ConnectError(quinn::ConnectError),
    ConnectionError(quinn::ConnectionError),
}

impl Display for VError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            VError::ServerNotFound(id) => write!(f, "server: {} not found;", id),
            VError::ServerNotStart(id) => write!(f, "server: {} not start", id),
            VError::SendError(msg) => write!(f, "send data error : {}", msg),
            VError::StdIO(err) => write!(f, "IO error: {}", err),
            VError::WriteError(err) => write!(f, "quic write error {}", err),
            VError::ConnectError(err) => write!(f, "quic connect error {}", err),
            VError::ConnectionError(err) => write!(f, "quic connection error {}", err),
        }
    }
}

impl Error for VError {}

impl From<std::io::Error> for VError {
    fn from(e: std::io::Error) -> Self {
        VError::StdIO(e)
    }
}

impl<T> From<SendError<T>> for VError {
    fn from(e: SendError<T>) -> Self {
        VError::SendError(format!("{}", e))
    }
}

impl From<quinn::WriteError> for VError {
    fn from(e: quinn::WriteError) -> Self {
        VError::WriteError(e)
    }
}

impl From<quinn::ConnectError> for VError {
    fn from(e: quinn::ConnectError) -> Self {
        VError::ConnectError(e)
    }
}

impl From<quinn::ConnectionError> for VError {
    fn from(e: quinn::ConnectionError) -> Self {
        VError::ConnectionError(e)
    }
}
