use dashmap::DashMap;
use reqwest::Client;
use serde::Deserialize;

use std::sync::Arc;

use crate::deserializer::Deserializer;
use crate::schema::{Format, Schema, SchemaDetails};
use crate::serializer::Serializer;
use crate::{Error, Result};

#[derive(Default)]
pub struct SchemaRegistry {
    schemas: DashMap<u32, Arc<Schema>>,
    subject_to_latest_id: DashMap<String, u32>,
    subject_version_to_id: DashMap<(String, u32), u32>,
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
            subject_to_latest_id: Default::default(),
            subject_version_to_id: Default::default(),
            http_client: client,
            url: registry_url,
        }
    }

    pub async fn get_serializer(&self, details: SchemaDetails) -> Result<Serializer> {
        let schema = self.get_schema_by_subject(&details).await?;
        match details.format {
            Format::Avro => Ok(Serializer::Avro { schema }),
            _ => unimplemented!("only avro is currently supported"),
        }
    }

    pub async fn get_deserializer(&self) -> Deserializer<'_> {
        Deserializer { registry: self }
    }

    pub async fn get_schema_by_subject(&self, schema_details: &SchemaDetails) -> Result<SchemaRef> {
        let subject = schema_details.generate_subject_name();
        let version = schema_details.version;

        // Check to see if we have the schema cached
        if let Some((id, schema)) = self.check_cache_for_schema(Some(&subject), version, None) {
            let resp = SchemaRef { schema, id };
            return Ok(resp);
        }

        // @TODO - Add children schemas, they currently do nothing
        // let mut child_schemas = Vec::with_capacity(schema_details.schema_references.len());
        // for sub_schema in schema_details.schema_references {
        //     let child_schema = self.get_schema(sub_schema).await?;
        //     child_schemas.push(child_schema);
        // }

        // We need to request the schema (or the ID) from the registry
        match version {
            Some(version) => {
                let (schema_id, schema) = self
                    .fetch_schema(SchemaQueryType::Version(&subject, version))
                    .await?;
                let schema = schema_details.format.parse_schema(&schema)?;
                self.subject_version_to_id
                    .insert((subject.clone(), version), schema_id);
                let resp = SchemaRef {
                    schema: Arc::new(schema),
                    id: schema_id,
                };
                self.schemas.insert(schema_id, Arc::clone(&resp.schema));
                Ok(resp)
            }
            None => {
                let (schema_id, schema) =
                    self.fetch_schema(SchemaQueryType::Latest(&subject)).await?;
                let schema = schema_details.format.parse_schema(&schema)?;
                self.subject_to_latest_id.insert(subject.clone(), schema_id);
                let resp = SchemaRef {
                    schema: Arc::new(schema),
                    id: schema_id,
                };
                self.schemas.insert(schema_id, Arc::clone(&resp.schema));
                Ok(resp)
            }
        }
    }

    pub async fn get_schema_by_id(&self, id: u32, format: Format) -> Result<SchemaRef> {
        if let Some((id, schema)) = self.check_cache_for_schema(None, None, Some(id)) {
            let resp = SchemaRef { schema, id };
            return Ok(resp);
        }
        let (id, schema) = self.fetch_schema(SchemaQueryType::Id(id)).await?;
        let schema = format.parse_schema(&schema)?;
        let resp = SchemaRef {
            schema: Arc::new(schema),
            id,
        };
        self.schemas.insert(id, Arc::clone(&resp.schema));
        Ok(resp)
    }
}

impl SchemaRegistry {
    /// Checks whether the cache currently contains the schema that's been asked for.
    ///
    /// It always priorities checking the ID first
    fn check_cache_for_schema(
        &self,
        subject: Option<&str>,
        version: Option<u32>,
        id: Option<u32>,
    ) -> Option<(u32, Arc<Schema>)> {
        if let Some(id) = id {
            let s = self.schemas.get(&id)?;
            return Some((id, Arc::clone(s.value())));
        }
        if let Some(subject) = subject {
            let id = if let Some(version) = version {
                self.subject_version_to_id
                    .get(&(subject.to_string(), version))
                    .map(|v| *v.value())
            } else {
                self.subject_to_latest_id.get(subject).map(|v| *v.value())
            }?;
            let s = self.schemas.get(&id)?;
            return Some((id, Arc::clone(s.value())));
        }
        None
    }

    /// Returns (Schema ID, Raw Schema)
    async fn fetch_schema(&self, query: SchemaQueryType<'_>) -> Result<(u32, String)> {
        match query {
            SchemaQueryType::Id(id) => {
                let url = format!("{}/schemas/ids/{}", self.url, id);

                let response = self.http_client.get(&url).send().await?;
                let resp = response.json::<SchemaRegistryResponse>().await?;
                Ok((id, resp.schema))
            }
            SchemaQueryType::Latest(subject) => {
                let url = format!("{}/subjects/{}/versions/latest", self.url, subject);
                let response = self.http_client.get(&url).send().await?;
                let resp = response.json::<SchemaRegistryResponse>().await?;
                if resp.id.is_none() {
                    return Err(Error::IDNotReturned);
                }
                Ok((resp.id.unwrap(), resp.schema))
            }
            SchemaQueryType::Version(subject, version) => {
                let url = format!("{}/subjects/{}/versions/{}", self.url, subject, version);
                let response = self.http_client.get(&url).send().await?;
                let resp = response.json::<SchemaRegistryResponse>().await?;
                if resp.id.is_none() {
                    return Err(Error::IDNotReturned);
                }
                Ok((resp.id.unwrap(), resp.schema))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct SchemaRegistryResponse {
    subject: Option<String>,
    id: Option<u32>,
    version: Option<u32>,
    schema: String,
}

#[derive(Debug)]
pub struct SchemaRef {
    pub(crate) schema: Arc<Schema>,
    pub(crate) id: u32,
}

pub enum SchemaQueryType<'a> {
    /// Fetch the Schema by the global ID
    Id(u32),
    /// Find the latest value of the schema with the given subject
    Latest(&'a str),
    /// Find the version of the schema with the given subject
    Version(&'a str, u32),
}
