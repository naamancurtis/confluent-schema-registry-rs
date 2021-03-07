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

    #[error("Expected to find data however there was not enough found to deserialize anything")]
    NoDataFound,

    #[error("Expected to find a magic byte with value 0, are you sure this data was correctly serialized for the schema registry?")]
    NoMagicByte,

    #[error("Deserialization of the provided type failed")]
    DeserializationFailed,

    #[error("Either the subject or the ID must be a valid value to find a schema")]
    InvalidInput
}
