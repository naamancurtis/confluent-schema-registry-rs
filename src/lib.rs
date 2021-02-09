mod error;
mod schema_registry;
mod schema;
mod encoder;

pub use error::SchemaRegistryError;
pub type SchemaRegistryResult<T> = std::result::Result<T, SchemaRegistryError>;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
