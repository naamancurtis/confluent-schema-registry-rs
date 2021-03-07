use anyhow::Result;
use serde::{Deserialize, Serialize};

use confluent_schema_registry::{Format, SchemaDetails, SchemaRegistry, SubjectNamingStrategy};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct Test {
    a: i64,
    b: String,
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

    let mut serializer = registry.get_serializer(&details).await?;
    let bytes = serializer.serialize(&test)?;
    assert_eq!(bytes[0], 0);

    let mut deserializer = registry.get_deserializer();
    let result = deserializer.deserialize(&bytes, Format::Avro).await?;
    assert_eq!(test, result);

    Ok(())
}
