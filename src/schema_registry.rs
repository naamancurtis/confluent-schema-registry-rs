use dashmap::DashMap;
use reqwest::Client;
use serde::Deserialize;

use std::sync::Arc;

use crate::encoder::Encoder;
use crate::schema::{Format, Schema, SchemaDetails};
use crate::{Error, Result};

#[derive(Default)]
pub struct SchemaRegistry {
    schemas: DashMap<String, DashMap<u32, Arc<Schema>>>,
    latest_id_mapper: DashMap<String, u32>,
    http_client: Client,
    url: String,
}

impl SchemaRegistry {
    pub fn new(registry_url: String) -> Self {
        Self::new_with_client(Default::default(), registry_url)
    }

    pub fn new_with_client(client: Client, registry_url: String) -> Self {
        Self {
            schemas: Default::default(),
            latest_id_mapper: Default::default(),
            http_client: client,
            url: registry_url,
        }
    }

    pub async fn get_encoder(&self, details: SchemaDetails) -> Result<Encoder> {
        let schema = self.get_schema(&details).await?;
        match details.format {
            Format::Avro => Ok(Encoder::Avro { schema }),
            _ => unimplemented!("only avro is currently supported"),
        }
    }

    async fn get_schema(&self, schema_details: &SchemaDetails) -> Result<SchemaRef> {
        let subject = schema_details.generate_subject_name();
        let id = schema_details
            .version
            .or_else(|| self.latest_id_mapper.get(&subject).map(|v| *v.value()));

        // See if we have the schema cached
        if let Some(id) = id {
            if let Some(schema) = self.check_cache_for_schema(&subject, id) {
                let resp = SchemaRef { schema, id };
                return Ok(resp);
            }
        }

        // @TODO - Add children schemas, they currently do nothing
        // let mut child_schemas = Vec::with_capacity(schema_details.schema_references.len());
        // for sub_schema in schema_details.schema_references {
        //     let child_schema = self.get_schema(sub_schema).await?;
        //     child_schemas.push(child_schema);
        // }

        // We need to request the schema (or the ID) from the registry
        let (schema_id, schema) = self.fetch_schema(&subject, schema_details.version).await?;
        let schema = schema_details.format.parse_schema(&schema)?;
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

    fn check_cache_for_schema(&self, subject: &str, id: u32) -> Option<Arc<Schema>> {
        let subject_schemas = self.schemas.get(subject)?;
        let sub_schemas = subject_schemas.value();
        let schema = sub_schemas.get(&id)?;
        let s = schema.value();
        Some(Arc::clone(s))
    }

    /// Returns (Schema ID, Raw Schema)
    async fn fetch_schema(&self, subject: &str, id: Option<u32>) -> Result<(u32, String)> {
        if let Some(id) = id {
            let url = format!("{}/schemas/ids/{}", self.url, id);

            let response = self.http_client.get(&url).send().await?;
            let resp = response.json::<SchemaRegistryResponse>().await?;
            return Ok((id, resp.schema));
        }
        let url = format!("{}/subjects/{}/versions/latest", self.url, subject);
        let response = self.http_client.get(&url).send().await?;
        let resp = response.json::<SchemaRegistryResponse>().await?;
        if resp.version.is_none() {
            return Err(Error::IDNotReturned);
        }

        Ok((resp.version.unwrap(), resp.schema))
    }
}

#[derive(Debug, Deserialize)]
struct SchemaRegistryResponse {
    name: Option<String>,
    version: Option<u32>,
    schema: String,
}

#[derive(Debug)]
pub struct SchemaRef {
    pub(crate) schema: Arc<Schema>,
    pub(crate) id: u32,
}
