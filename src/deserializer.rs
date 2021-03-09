use futures_locks::RwLock;
use std::io::Cursor;

use serde::de::DeserializeOwned;

use crate::{
    schema::{Format, Schema},
    schema_registry::SchemaRef,
};
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
        if data[0] != 0 {
            return Err(Error::NoMagicByte);
        }
        let id = [data[1], data[2], data[3], data[4]];
        let id = u32::from_be_bytes(id);
        let raw_data = &data[5..];
        match format {
            Format::Avro => {
                let schema_ref = self.registry.get_schema_by_id(id, format).await?;
                deserialize_avro(&schema_ref, &raw_data)
            }
        }
    }
}

#[derive(Clone)]
pub struct CachedDeserializer<'a> {
    pub(crate) registry: &'a SchemaRegistry,
    pub(crate) schema: RwLock<Option<SchemaRef>>,
}

impl<'a> CachedDeserializer<'a> {
    pub async fn deserialize<D: DeserializeOwned>(&self, data: &[u8], format: Format) -> Result<D> {
        if data.len() < 5 {
            return Err(Error::NoDataFound);
        }
        if data[0] != 0 {
            return Err(Error::NoMagicByte);
        }
        let id = [data[1], data[2], data[3], data[4]];
        let id = u32::from_be_bytes(id);
        let raw_data = &data[5..];
        match format {
            Format::Avro => loop {
                {
                    let handle = self.schema.read().await;
                    if let Some(ref schema_ref) = *handle {
                        return deserialize_avro(schema_ref, &raw_data);
                    }
                }
                {
                    let schema_ref = self.registry.get_schema_by_id(id, format).await?;
                    let mut handle = self.schema.write().await;
                    *handle = Some(schema_ref);
                }
            },
        }
    }
}

fn deserialize_avro<D: DeserializeOwned>(schema_ref: &SchemaRef, data: &[u8]) -> Result<D> {
    if let Schema::Avro(ref s) = &*schema_ref.schema {
        let mut reader = Cursor::new(data);
        let value = avro_rs::from_avro_datum(&s, &mut reader, None)?;
        let final_value = avro_rs::from_value::<D>(&value)?;
        Ok(final_value)
    } else {
        Err(Error::IncorrectSchemaType(
            "Avro".to_owned(),
            schema_ref.schema.schema_type().to_string(),
        ))
    }
}
