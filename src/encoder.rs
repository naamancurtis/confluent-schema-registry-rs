use serde::Serialize;

use crate::schema::Schema;

pub trait Encoder<T>
where
    T: Sized + Serialize,
{
    fn encode(&self, data: T) -> Vec<u8>;
    fn parse_schema(&self, schema: &str) -> Schema;
}
