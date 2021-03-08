use avro_rs::Reader;
use serde::de::DeserializeOwned;

use crate::schema::{Format, Schema};
use crate::{Error, Result, SchemaRegistry};

#[derive(Clone, Copy)]
pub struct Deserializer<'a> {
    pub(crate) registry: &'a SchemaRegistry,
}

impl<'a> Deserializer<'a> {
    pub async fn deserialize<D: DeserializeOwned>(&self, data: &[u8], format: Format) -> Result<D> {
        if data.len() < 5 {
            return Err(Error::NoDataFound);
        }
        let magic_byte = data[0];
        if magic_byte != 0 {
            return Err(Error::NoMagicByte);
        }
        let id = [data[1], data[2], data[3], data[4]];
        let id = u32::from_be_bytes(id);
        let raw_data = &data[5..];
        match format {
            Format::Avro => {
                let schema = self.registry.get_schema_by_id(id, format).await?;
                if let Schema::Avro(ref s) = &*schema.schema {
                    let mut val = Reader::with_schema(&s, raw_data)?;
                    if let Some(v) = val.next() {
                        let final_value = avro_rs::from_value::<D>(&v?)?;
                        return Ok(final_value);
                    }
                    Err(Error::DeserializationFailed)
                } else {
                    Err(Error::IncorrectSchemaType(
                        "Avro".to_owned(),
                        schema.schema.schema_type().to_string(),
                    ))
                }
            }
        }
    }
}
