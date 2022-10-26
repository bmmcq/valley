use thiserror::Error;

use crate::ServerId;

#[derive(Error, Debug)]
pub enum ConnectError {
    #[error("connect failed because {source};")]
    SystemIO {
        #[from]
        source: std::io::Error,
    },
    #[error("the server with id={0} not found;")]
    ServerNotFound(ServerId),
    #[error("fail to resolve address of server with id = {0} because of {source};")]
    FetchServerAddr { server_id: ServerId, source: anyhow::Error },
    #[error("create quic connection failed because {source};")]
    QUICConnect {
        #[from]
        source: quinn::ConnectError,
    },
    #[error("quic connection abnormal {source};")]
    QUICConnection {
        #[from]
        source: quinn::ConnectionError,
    },
    #[error("fail send channel's handshake on quic connection, because {source}")]
    QUICConnHandShake {
        #[from]
        source: quinn::WriteError,
    },
    #[error("unknown connection error {source:?}")]
    Unknown {
        #[from]
        source: anyhow::Error,
    },
}
