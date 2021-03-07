# Confluent Schema Registry

This crate is designed to make working with Apache Kafka and the Confluent
Schema Registry easier. Under the hood is interacting with the Schema Registry
via it's REST API.

Currently, this crate is mainly designed/built out to meet the requirements I have for applications I'm building,
although I will try _(and am open to contributions)_ to build this out to
include more functionality. It is also very much a WIP.

For a more feature complete crate, check out [Schema Registry Converter](https://github.com/gklijs/schema_registry_converter).

The primary difference between these two crates is that this one was designed to
be used in multi-threaded environments and to minimize network traffic between
your application and the registry.

The main idea is that you will have one instance
of the `SchemaRegistry` held somewhere in your application, and you can create,
clone and destroy `Serializers` and `Deserializers` as and when you need, with
very little overhead

## Basic Example

```rust
use confluent_schema_registry::{Format, SchemaDetails, SchemaRegistry, SubjectNamingStrategy};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct Test {
    a: i64,
    b: String,
}

#[tokio::main]
async fn main() -> Result<()> {
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
    // If the schemas already exist in the registry, there is no need to repost
    // them
    // However this is idempotent does have the nice side effect of caching the schemas
    registry.post_schemas_to_registry(&schemas).await?;

    let test = Test {
        a: 100,
        b: String::from("My Test"),
    };

    let mut serializer = registry.get_serializer(&details).await?;
    let bytes = serializer.serialize(&test)?;

    let mut deserializer = registry.get_deserializer();
    let result = deserializer.deserialize(&bytes, Format::Avro).await?;
}
```
