use anyhow::Result;
use proto::tuples::{
    tuples_client::TuplesClient, Empty, GetSchemaRequest, RegisterSchemaRequest,
};

const SERVER: &str = "http://[::1]:50051";

pub async fn get_version() -> Result<String> {
    let mut client = TuplesClient::connect(SERVER).await?;
    let resp = client.get_version(Empty {}).await?;
    Ok(resp.into_inner().version)
}

pub async fn list_schemas() -> Result<Vec<String>> {
    let mut client = TuplesClient::connect(SERVER).await?;
    let resp = client.list_schemas(Empty {}).await?;
    Ok(resp.into_inner().schemas.into_iter().map(|s| s.name).collect())
}

pub async fn get_schema(name: String) -> Result<String> {
    let mut client = TuplesClient::connect(SERVER).await?;
    let resp = client.get_schema(GetSchemaRequest { name }).await?;
    Ok(resp.into_inner().definition)
}

pub async fn register_schema(name: String, definition: String) -> Result<()> {
    let mut client = TuplesClient::connect(SERVER).await?;
    client.register_schema(RegisterSchemaRequest { name, definition }).await?;
    Ok(())
}
