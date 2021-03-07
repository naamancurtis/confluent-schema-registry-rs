use avro_rs::Writer;
use serde::Serialize;

use crate::schema::Schema;
use crate::schema_registry::SchemaRef;
use crate::{Error, Result};

pub enum Serializer {
    Avro { schema: SchemaRef },
}

impl Serializer {
    pub fn serialize<S: Serialize>(&mut self, data: S) -> Result<Vec<u8>> {
        match *self {
            Self::Avro { ref schema } => {
                // Add magic bytes
                let id = schema.id;
                if let Schema::Avro(ref s) = &*schema.schema {
                    let size = std::mem::size_of::<S>();
                    let mut w = Writer::new(&s, Vec::with_capacity(size));
                    w.append_ser(data)?;
                    let mut serialized_data = w.into_inner()?;
                    let bytes = add_magic_byte_and_schema_id(&mut serialized_data, id);
                    Ok(bytes)
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
