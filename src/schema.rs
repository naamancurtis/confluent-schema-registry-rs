#[cfg(features = "avro")]
use avro_rs::Schema as AvroSchema;

use crate::SchemaRegistryResult;

#[derive(Debug, Clone)]
pub struct SchemaDetails {
    /// The version of the schema you would like to retrieve, leave as `None` to fetch the latest
    pub version: Option<u32>,
    pub subject_naming_strategy: SubjectNamingStrategy,
    pub is_key: bool,
    /// A list of other schemas that are required from the registry to resolve this one
    ///
    /// Any time calls to the schema registry are made to fetch schemas, these schema references
    /// will be resolved first. Please be **careful** with recursion here.
    pub schema_references: Vec<Box<SchemaDetails>>,
}

impl SchemaDetails {
    pub fn generate_subject_name(&self) -> String {
        match self.subject_naming_strategy {
            SubjectNamingStrategy::SubjectNameStrategy { is_key, subject } => {
                let suffix = if is_key { "key" } else { "value" };
                format!("{}-{}", subject, suffix)
            }
            SubjectNamingStrategy::TopicNameStrategy { topic_name } => topic_name.clone(),
            SubjectNamingStrategy::RecordNameStrategy { message_type_name } => {
                message_type_name.clone()
            }
            SubjectNamingStrategy::TopicRecordNameStrategy {
                topic_name,
                message_type_name,
            } => format!("{}-{}", topic_name, message_type_name),
            SubjectNamingStrategy::Custom(s) => s.clone(),
        }
    }
}

/// There are serialization format specific behaviors that occur within the Schema Registry
///
/// If you aren't already aware of them, please check each invariant below to find out how it behaves
///
/// For more detailed reading, you can find confluents documentation here:
/// - [Avro](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html#serdes-and-formatter-avro)
/// - [Protobuf](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-protobuf.html#serdes-and-formatter-protobuf)
/// - [JSON](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html#serdes-and-formatter-json)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubjectNamingStrategy {
    /// # Serialization format specifics
    ///
    /// ## Avro
    ///
    /// - If the `key` of the message is being encoded, the subject will be suffixed with `-key`
    /// - If the `value` of the message is being encoded, the subject will be suffixed with `-value`
    ///
    /// The primary driver behind this is because Avro Serialization supports the encoding of both
    /// the key, and the value (if desired) so the schemas must be under different resource paths.
    ///
    /// ### Example
    ///
    /// If your subject is **order**
    /// - Then the `key` would be stored in the schema registry with a subject of `order-key`
    /// - Then the `value` would be stored in the schema registry with a subject of `order-value`
    SubjectNameStrategy {
        /// This defines the **Resource Name** of this schema within the schema registry
        subject: String,
        is_key: bool,
    },
    // For Protobuf, the message name.
    // For JSON Schema, the title.
    RecordNameStrategy {
        /// This name depends on the serialization format of the root type for this message
        /// It should also be the fully qualified name for that type (within the bounds of whatever
        /// schema language you're using).
        ///
        /// _This currently isn't enforced by any checking, so if you want it to align with the
        /// Schema Registry versioning you'll have to make sure this is right_
        ///
        /// - For Avro, this will usually be the record name.
        message_type_name: String,
    },
    TopicNameStrategy {
        topic_name: String,
    },
    TopicRecordNameStrategy {
        topic_name: String,
        /// This name depends on the serialization format of the root type for this message
        ///
        /// It should also be the fully qualified name for that type (within the bounds of whatever
        ///
        /// _This currently isn't enforced by any checking, so if you want it to align with the
        /// Schema Registry versioning you'll have to make sure this is right_
        ///
        /// - For Avro, the record name.
        message_type_name: String,
    },
    /// Allows you to specify the exact name you would like your schema to be registered under
    Custom(String),
}

#[derive(Debug, PartialEq)]
pub enum Schema {
    Protobuf(i32),
    Avro(AvroSchema),
}

impl Schema {
    pub fn new_avro_schema(schema: &str) -> SchemaRegistryResult<Self> {
        let sch = AvroSchema::parse_str(schema)?;
        Ok(Self::Avro(sch))
    }
}
