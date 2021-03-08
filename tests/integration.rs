use anyhow::Result;
use schema_registry_converter::async_impl::avro::AvroEncoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy as Strat;
use serde::{Deserialize, Serialize};

use confluent_schema_registry::{Format, SchemaDetails, SchemaRegistry, SubjectNamingStrategy};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct Test {
    a: i64,
    b: String,
}

/// The `stream_id` is associated with this data via the `Key` for the kafka message, which is why
/// it is not embedded in this struct
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub user_id: String,
    pub event_type: MetadataEventType,
    pub start_time: i64,
    pub channel_admin: bool,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            user_id: "ANONYMOUS".to_string(),
            event_type: Default::default(),
            start_time: 1234567,
            channel_admin: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Default, PartialEq)]
pub struct Message {
    pub id: u32,
    pub message: String,
    pub title: Option<String>,
    pub viewed: bool,
    #[serde(skip_serializing)]
    pub timestamp: Option<i64>,
}

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum MetadataEventType {
    Join,
    Leave,
    Message,
}

impl Default for MetadataEventType {
    fn default() -> Self {
        Self::Message
    }
}

#[tokio::test]
async fn it_works() -> Result<()> {
    let registry = SchemaRegistry::new("http://localhost:8081".to_owned());
    let raw_schema = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"}
        ]
    }
    "#;
    let details = SchemaDetails {
        version: None,
        subject_naming_strategy: SubjectNamingStrategy::TopicNameStrategy {
            topic_name: "my-topic".into(),
            is_key: false,
        },
        schema_references: Default::default(),
        format: Default::default(),
    };
    let schemas = vec![(raw_schema, &details)];
    registry.post_schemas_to_registry(&schemas).await?;

    let test = Test {
        a: 100,
        b: String::from("My Test"),
    };

    let serializer = registry.get_serializer(&details).await?;
    let bytes = serializer.serialize(&test)?;
    assert_eq!(bytes[0], 0);

    let deserializer = registry.get_deserializer();
    let result = deserializer.deserialize(&bytes, Format::Avro).await?;
    assert_eq!(test, result);

    Ok(())
}

#[tokio::test]
async fn it_works_2() -> Result<()> {
    let mut encoder = AvroEncoder::new(SrSettings::new("http://localhost:8081".to_string()));
    let registry = SchemaRegistry::new("http://localhost:8081".to_owned());

    let raw_schema = r#"{
  "type": "record",
  "name": "Metadata",
  "namespace": "awesome.chatroom",
  "doc": "Metadata Events",
  "fields": [
    {
      "name": "user_id",
      "type": "string"
    },
    {
      "name": "event_type",
      "type": {
        "name": "MetadataEventType",
        "type": "enum",
        "symbols": ["Join", "Leave", "Message"]
      }
    },
    {
      "name": "start_time",
      "type": "long",
      "logicalType": "timestamp-millis"
    },
    {
      "name": "channel_admin",
      "type": "boolean"
    }
  ]
}
"#;
    let topic_name = "metadata";
    let details = SchemaDetails {
        version: None,
        subject_naming_strategy: SubjectNamingStrategy::TopicNameStrategy {
            topic_name: topic_name.to_string(),
            is_key: false,
        },
        schema_references: Default::default(),
        format: Default::default(),
    };
    let schemas = vec![(raw_schema, &details)];
    registry.post_schemas_to_registry(&schemas).await?;

    let test = Metadata::default();

    let encoded_message = encoder
        .encode_struct(
            test.clone(),
            &Strat::TopicNameStrategy(topic_name.to_string(), false),
        )
        .await
        .expect("failed to use lib");

    let serializer = registry.get_serializer(&details).await?;
    let bytes = serializer.serialize(&test)?;
    assert_eq!(bytes[0], 0);
    assert_eq!(encoded_message, bytes);

    let deserializer = registry.get_deserializer();
    let result = deserializer.deserialize(&bytes, Format::Avro).await?;
    assert_eq!(test, result);

    Ok(())
}

#[tokio::test]
async fn it_works_3() -> Result<()> {
    let mut encoder = AvroEncoder::new(SrSettings::new("http://localhost:8081".to_string()));
    let registry = SchemaRegistry::new("http://localhost:8081".to_owned());
    let raw_schema = r#"{
  "type": "record",
  "name": "Event",
  "namespace": "awesome.chatroom",
  "doc": "Events",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "message",
      "type": "string"
    },
    {
      "name": "title",
      "type": ["null", "string"] 
    },
    {
      "name": "viewed",
      "type": "boolean"
    }
  ]
}
"#;

    let topic_name = "events";
    let details = SchemaDetails {
        version: None,
        subject_naming_strategy: SubjectNamingStrategy::TopicNameStrategy {
            topic_name: topic_name.to_string(),
            is_key: false,
        },
        schema_references: Default::default(),
        format: Default::default(),
    };
    let schemas = vec![(raw_schema, &details)];
    registry.post_schemas_to_registry(&schemas).await?;

    let mut test = Message {
        id: 1,
        title: Some("My awesome Message".to_string()),
        message: "Really insightful and thought provoking message".to_string(),
        viewed: false,
        timestamp: Some(123871591),
    };

    let encoded_message = encoder
        .encode_struct(
            test.clone(),
            &Strat::TopicNameStrategy(topic_name.to_string(), false),
        )
        .await
        .expect("failed to use lib");

    let serializer = registry.get_serializer(&details).await?;
    let bytes = serializer.serialize(&test)?;
    assert_eq!(bytes[0], 0);
    assert_eq!(encoded_message, bytes);

    let deserializer = registry.get_deserializer();
    let result = deserializer
        .deserialize(&encoded_message, Format::Avro)
        .await?;
    // We don't serialize the timestamp
    test.timestamp = None;
    assert_eq!(test, result);

    Ok(())
}
