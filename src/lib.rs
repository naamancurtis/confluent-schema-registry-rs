mod deserializer;
mod error;
mod schema;
mod schema_registry;
mod serializer;

pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;
pub use deserializer::{CachedDeserializer, Deserializer};
pub use schema::{Format, SchemaDetails, SubjectNamingStrategy};
pub use schema_registry::SchemaRegistry;
pub use serializer::Serializer;

#[cfg(feature = "avro")]
pub use avro_rs as avro;
