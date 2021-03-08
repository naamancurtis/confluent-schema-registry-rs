use dashmap::DashMap;
use futures_locks::RwLock;
use lazy_static::lazy_static;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, CONTENT_TYPE};
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use std::sync::Arc;

use crate::deserializer::{CachedDeserializer, Deserializer};
use crate::schema::{Format, Schema, SchemaDetails};
use crate::serializer::Serializer;
use crate::{Error, Result};

lazy_static! {
    static ref HEADERS: HeaderMap = {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.schemaregistry.v1+json"),
        );
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.schemaregistry.v1+json"),
        );
        headers
    };
}

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

    /// Generate a serializer that is ready to serialize a type with the provided schema
    pub async fn get_serializer(&self, details: &SchemaDetails) -> Result<Serializer> {
        let schema = self.get_schema_by_subject(details).await?;
        match details.format {
            Format::Avro => Ok(Serializer::Avro { schema }),
            _ => unimplemented!("only avro is currently supported"),
        }
    }

    /// Generate a deserializer that is ready to deserialize any bytes which have previously been
    /// encoded with the Confluent Schema Registry protocol
    pub fn get_deserializer(&self) -> Deserializer<'_> {
        Deserializer { registry: self }
    }

    /// Generate a deserializer that is ready to deserialize any bytes which have previously been
    /// encoded with the Confluent Schema Registry protocol
    ///
    /// This deserializer will cache the schema of the first type it deserializes, and use that for
    /// all subsequent deserilizations.
    ///
    /// This only really has the use case when you know you are only going to be deserializing one
    /// type, and they all use the exact same version of the schema, however when this case is
    /// true, this will allow you to reduce the network traffic for deserialization
    pub fn get_cached_deserializer(&self) -> CachedDeserializer<'_> {
        CachedDeserializer {
            registry: self,
            schema: RwLock::new(None),
        }
    }

    pub(crate) async fn get_schema_by_subject(
        &self,
        schema_details: &SchemaDetails,
    ) -> Result<SchemaRef> {
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

    pub(crate) async fn get_schema_by_id(&self, id: u32, format: Format) -> Result<SchemaRef> {
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

    /// Takes a reference to a slice of raw schema strings and their corresponding schema details
    /// and posts them to the schema registry, this also pre-populates the client with the
    /// identification details of all of those schemas
    pub async fn post_schemas_to_registry(&self, schemas: &[(&str, &SchemaDetails)]) -> Result<()> {
        for (schema, details) in schemas {
            let url = format!(
                "{}/subjects/{}/versions",
                self.url,
                details.generate_subject_name()
            );
            let req = SchemaRegistryRequest {
                schema,
                schema_type: details.format,
            };
            // I don't really like this, but this call is required to add a NEW schema
            // however it doesn't return a full set of information, so we basically ignore it
            match self
                .post_schema::<SchemaRegistryPostResponse>(&url, &req)
                .await
            {
                Ok(_) => {}
                Err(e) => return Err(e),
            };
            let url = format!("{}/subjects/{}", self.url, details.generate_subject_name());
            // This call actually gives us the information we need, however it won't add a schema
            // if it doesn't already exist
            let schema = self
                .post_schema::<SchemaRegistryResponse>(&url, &req)
                .await
                .map(parse_post_response)??;
            self.parse_response(schema, details.format, details.version)?;
        }
        Ok(())
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
                let (_, schema) = self.get_schema(&url).await?;
                Ok((id, schema))
            }
            SchemaQueryType::Latest(subject) => {
                let url = format!("{}/subjects/{}/versions/latest", self.url, subject);
                let (id, schema) = self.get_schema(&url).await?;
                if id.is_none() {
                    return Err(Error::IDNotReturned);
                }
                Ok((id.unwrap(), schema))
            }
            SchemaQueryType::Version(subject, version) => {
                let url = format!("{}/subjects/{}/versions/{}", self.url, subject, version);
                let (id, schema) = self.get_schema(&url).await?;
                if id.is_none() {
                    return Err(Error::IDNotReturned);
                }
                Ok((id.unwrap(), schema))
            }
        }
    }

    async fn get_schema(&self, url: &str) -> Result<(Option<u32>, String)> {
        let response = self
            .http_client
            .get(url)
            .headers(HEADERS.clone())
            .send()
            .await?
            .json::<SchemaRegistryResponse>()
            .await
            .map(|resp| parse_post_response(resp).map(|data| (data.id, data.schema)))??;
        Ok(response)
    }

    async fn post_schema<D: DeserializeOwned>(
        &self,
        url: &str,
        req: &SchemaRegistryRequest<'_>,
    ) -> Result<D> {
        let response = self
            .http_client
            .post(url)
            .headers(HEADERS.clone())
            .json(req)
            .send()
            .await?
            .json::<D>()
            .await?;
        Ok(response)
    }

    fn parse_response(
        &self,
        response: SchemaRegistryData,
        format: Format,
        version: Option<u32>,
    ) -> Result<()> {
        let parsed_schema = format.parse_schema(&response.schema)?;
        if let Some(id) = response.id {
            self.schemas.insert(id, Arc::new(parsed_schema));
            if let Some(subject) = response.subject {
                if version.is_none() {
                    self.subject_to_latest_id.insert(subject.clone(), id);
                }
                if let Some(version) = response.version {
                    self.subject_version_to_id.insert((subject, version), id);
                }
            }
        }
        Ok(())
    }
}

fn parse_post_response(mut response: SchemaRegistryResponse) -> Result<SchemaRegistryData> {
    if let Some(data) = response.data.take() {
        return Ok(data);
    }
    if let Some(error) = response.error.take() {
        return Err(Error::SchemaRegistryError {
            error_code: error.error_code,
            message: error
                .message
                .unwrap_or_else(|| "Unexpected error from the schema registry".to_owned()),
        });
    }
    Err(Error::UnexpectedError)
}

#[derive(Debug, Serialize)]
#[serde(rename_all(serialize = "camelCase"))]
struct SchemaRegistryRequest<'a> {
    schema: &'a str,
    schema_type: Format,
    // references // @TODO
}

#[derive(Debug, Deserialize)]
struct SchemaRegistryResponse {
    #[serde(flatten)]
    data: Option<SchemaRegistryData>,
    #[serde(flatten)]
    error: Option<SchemaRegistryError>,
}

#[derive(Debug, Deserialize)]
struct SchemaRegistryPostResponse {
    id: u32,
}

#[derive(Debug, Deserialize)]
struct SchemaRegistryData {
    subject: Option<String>,
    id: Option<u32>,
    version: Option<u32>,
    schema: String,
}

#[derive(Debug, Deserialize)]
struct SchemaRegistryError {
    error_code: u32,
    message: Option<String>,
}

#[derive(Debug, Clone)]
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
