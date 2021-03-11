use futures_locks::RwLock;
use serde::de::DeserializeOwned;

use std::io::Cursor;
use std::sync::Arc;

use crate::schema::{Format, Schema};
use crate::schema_registry::SchemaRef;
use crate::{Error, Result, SchemaRegistry};

#[derive(Clone, Copy)]
pub struct Deserializer<'a> {
    pub(crate) registry: &'a SchemaRegistry,
}

impl<'a> Deserializer<'a> {
    pub async fn deserialize<D: DeserializeOwned>(&self, data: &[u8], format: Format) -> Result<D> {
        deserialize_uncached(self, data, format).await
    }
}

#[derive(Clone)]
pub struct CachedDeserializer<'a> {
    pub(crate) registry: &'a SchemaRegistry,
    pub(crate) schema: RwLock<Option<SchemaRef>>,
}

impl<'a> CachedDeserializer<'a> {
    pub async fn deserialize<D: DeserializeOwned>(&self, data: &[u8], format: Format) -> Result<D> {
        deserialize_cached(self, data, format).await
    }
}

#[derive(Clone)]
pub struct ArcDeserializer {
    pub(crate) registry: Arc<SchemaRegistry>,
}

impl ArcDeserializer {
    pub fn new(registry: Arc<SchemaRegistry>) -> Self {
        Self { registry }
    }

    pub async fn deserialize<D: DeserializeOwned>(&self, data: &[u8], format: Format) -> Result<D> {
        deserialize_uncached(self, data, format).await
    }
}

#[derive(Clone)]
pub struct ArcCachedDeserializer {
    pub(crate) registry: Arc<SchemaRegistry>,
    pub(crate) schema: RwLock<Option<SchemaRef>>,
}

impl ArcCachedDeserializer {
    pub fn new(registry: Arc<SchemaRegistry>) -> Self {
        Self {
            registry,
            schema: RwLock::new(None),
        }
    }

    pub async fn deserialize<D: DeserializeOwned>(&self, data: &[u8], format: Format) -> Result<D> {
        deserialize_cached(self, data, format).await
    }
}

trait DeserializeUncached {
    fn get_registry(&self) -> &SchemaRegistry;
}

impl DeserializeUncached for ArcDeserializer {
    fn get_registry(&self) -> &SchemaRegistry {
        &self.registry
    }
}

impl<'a> DeserializeUncached for Deserializer<'a> {
    fn get_registry(&self) -> &SchemaRegistry {
        &self.registry
    }
}

trait DeserializeCached {
    fn get_schema(&self) -> &RwLock<Option<SchemaRef>>;
    fn get_registry(&self) -> &SchemaRegistry;
}

impl DeserializeCached for ArcCachedDeserializer {
    fn get_schema(&self) -> &RwLock<Option<SchemaRef>> {
        &self.schema
    }

    fn get_registry(&self) -> &SchemaRegistry {
        &self.registry
    }
}

impl<'a> DeserializeCached for CachedDeserializer<'a> {
    fn get_schema(&self) -> &RwLock<Option<SchemaRef>> {
        &self.schema
    }

    fn get_registry(&self) -> &SchemaRegistry {
        &self.registry
    }
}
async fn deserialize_uncached<D: DeserializeOwned>(
    this: &impl DeserializeUncached,
    data: &[u8],
    format: Format,
) -> Result<D> {
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
            let schema_ref = this.get_registry().get_schema_by_id(id, format).await?;
            deserialize_avro(&schema_ref, &raw_data)
        }
    }
}

async fn deserialize_cached<D: DeserializeOwned>(
    this: &impl DeserializeCached,
    data: &[u8],
    format: Format,
) -> Result<D> {
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
                let handle = this.get_schema().read().await;
                if let Some(ref schema_ref) = *handle {
                    return deserialize_avro(schema_ref, &raw_data);
                }
            }
            {
                if let Ok(mut handle) = this.get_schema().try_write() {
                    let schema_ref = this.get_registry().get_schema_by_id(id, format).await?;
                    *handle = Some(schema_ref);
                }
            }
        },
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
