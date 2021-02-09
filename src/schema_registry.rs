use dashmap::DashMap;
use reqwest::Client;
use serde::Serialize;

use crate::encoder::Encoder;
use crate::schema::{Schema, SchemaDetails};

#[derive(Default)]
pub struct SchemaRegistry {
    schemas: DashMap<(u32, String), Schema>,
    http_client: Client,
}

impl SchemaRegistry {
    pub fn new_with_client(client: Client) -> Self {
        Self {
            schemas: Default::default(),
            http_client: client,
        }
    }

    pub fn get_encoder<T>(schema: SchemaDetails) -> Box<dyn Encoder<T>>
    where
        T: Sized + Serialize,
    {
        todo!()
    }

    fn get_schema(&self, schema: &SchemaDetails) -> &Schema {
        let subject = schema.generate_subject_name();
        todo!()
    }
}
