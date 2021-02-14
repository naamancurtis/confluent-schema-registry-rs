#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[cfg(feature = "avro")]
    #[error(transparent)]
    Avro(#[from] avro_rs::Error),

    #[error(transparent)]
    Http(#[from] reqwest::Error),
}
