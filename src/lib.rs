mod encoder;
mod error;
mod schema;
mod schema_registry;

pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(feature = "avro")]
pub use avro_rs as avro;
