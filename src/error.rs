#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[cfg(feature = "avro")]
    #[error(transparent)]
    Avro(#[from] avro_rs::Error),

    #[error(transparent)]
    Http(#[from] reqwest::Error),

    #[error("Expected to recieve a Schema ID from the registry but found nothing")]
    IDNotReturned,

    #[error("Expected to find a schema with the type {0}, but found one with {1}")]
    IncorrectSchemaType(String, String),
}
