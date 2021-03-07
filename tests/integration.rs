use anyhow::Result;

use confluent_schema_registry::SchemaRegistry;

#[tokio::test]
async fn it_works() -> Result<()> {
    let registry = SchemaRegistry::new("http://localhost:8081");

}
