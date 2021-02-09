use thiserror::Error;

#[derive(Debug, Error)]
pub enum SchemaRegistryError {
    #[cfg(feature = "avro")]
    Avro(#[from] avro_rs::Error),
}
