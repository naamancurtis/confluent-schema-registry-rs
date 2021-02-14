use dashmap::DashMap;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use std::sync::Arc;

use crate::encoder::Encoder;
use crate::schema::{Schema, SchemaDetails};
use crate::Result;

#[derive(Default)]
pub struct SchemaRegistry {
    schemas: DashMap<String, DashMap<u32, Arc<Schema>>>,
    latest_id_mapper: DashMap<String, u32>,
    http_client: Client,
    url: String,
}

impl SchemaRegistry {
    pub fn new_with_client(client: Client, root_url: String) -> Self {
        Self {
            schemas: Default::default(),
            latest_id_mapper: Default::default(),
            http_client: client,
            url: root_url,
        }
    }

    pub fn get_encoder<T>(schema: SchemaDetails) -> Box<dyn Encoder<T>>
    where
        T: Sized + Serialize,
    {
        todo!()
    }

    async fn get_schema<E, T>(
        &self,
        schema_details: &SchemaDetails,
        encoder: E,
    ) -> Result<SchemaRef>
    where
        E: Encoder<T>,
        T: Sized + Serialize,
    {
        let subject = schema_details.generate_subject_name();
        let id = schema_details
            .version
            .or_else(|| self.latest_id_mapper.get(&subject).map(|v| *v.value()));

        // See if we have the schema cached
        if let Some(id) = id {
            if let Some(schema) = self.check_cache_for_schema(subject.clone(), id) {
                let resp = SchemaRef { schema, id };
                return Ok(resp);
            }
        }

        // We need to request the schema (or the ID) from the registry
        let (schema_id, schema) = self.fetch_schema(&subject, schema_details.version).await?;
        let schema = encoder.parse_schema(&schema);
        if id.is_none() {
            self.latest_id_mapper.insert(subject.clone(), schema_id);
        }
        let resp = SchemaRef {
            schema: Arc::new(schema),
            id: schema_id,
        };
        // Hold the write handle for the shortest amount of time possible
        {
            let entry = self.schemas.entry(subject.clone()).or_default();
            entry.insert(schema_id, Arc::clone(&resp.schema));
        }

        Ok(resp)
    }

    fn check_cache_for_schema(&self, subject: String, id: u32) -> Option<Arc<Schema>> {
        let subject_schemas = self.schemas.get(&subject)?;
        let sub_schemas = subject_schemas.value();
        let schema = sub_schemas.get(&id)?;
        let s = schema.value();
        Some(Arc::clone(s))
    }

    /// Returns (Schema ID, Raw Schema)
    async fn fetch_schema(&self, subject: &str, id: Option<u32>) -> Result<(u32, String)> {
        let url = match id {
            Some(id) => format!("{}/schemas/ids/{}", self.url, id),
            None => format!("{}/subjects/{}/versions/latest", self.url, subject),
        };
        let response = self.http_client.get(&url).send().await?;
        let resp = response.json::<SchemaRegistryResponse>().await?;
        Ok((resp.version, resp.schema.to_owned()))
    }
}

#[derive(Debug, Deserialize)]
struct SchemaRegistryResponse {
    name: String,
    version: u32,
    schema: String,
}

#[derive(Debug)]
pub struct SchemaRef {
    schema: Arc<Schema>,
    id: u32,
}
