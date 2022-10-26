use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerStartError {
    #[error("server start failed because of : {source}")]
    SystemIO {
        #[from]
        source: std::io::Error
    },
    #[error("server register failed because of : {source}")]
    ServerRegister {
        #[from]
        source: anyhow::Error
    }
}