use serde::Serialize;

use crate::schema::Schema;
use crate::schema_registry::SchemaRef;
use crate::{Error, Result};

#[derive(Clone)]
pub enum Serializer {
    Avro { schema: SchemaRef },
}

impl Serializer {
    pub fn serialize<S: Serialize>(&self, data: S) -> Result<Vec<u8>> {
        match *self {
            Self::Avro { ref schema } => {
                // Add magic bytes
                let id = schema.id;
                if let Schema::Avro(ref s) = &*schema.schema {
                    let value = avro_rs::to_value(data)?;
                    let mut bytes = avro_rs::to_avro_datum(&s, value)?;
                    let serialized_bytes = add_magic_byte_and_schema_id(&mut bytes, id);
                    Ok(serialized_bytes)
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

fn add_magic_byte_and_schema_id(payload: &mut Vec<u8>, id: u32) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(payload.len() + 5);
    bytes.push(0);
    bytes.append(&mut id.to_be_bytes().to_vec());
    bytes.append(payload);
    bytes
}
