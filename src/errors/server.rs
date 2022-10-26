use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("current server not started;")]
    NotStart,
    #[error("server start failed because of : {source}")]
    StartError {
        #[from]
        source: std::io::Error,
    },
    #[error("server register failed because of : {source}")]
    RegisterError {
        #[from]
        source: anyhow::Error,
    },
}
