#![deny(warnings)]

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use proto::tuples::{
    tuples_client::TuplesClient, Empty, GetPlaybookRequest, GetSchemaRequest, GetTupleRequest,
    MatchTupleRequest, PutTupleRequest, RegisterFilterRequest, RegisterPlaybookRequest,
    RegisterSchemaRequest, WatchTriggersRequest,
};

const SERVER_ADDR: &str = "http://[::1]:50051";

#[derive(Parser)]
#[command(name = "tuples", about = "CLI for the tuples server")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print the server version
    Version,
    /// Manage schemas
    Schema {
        #[command(subcommand)]
        action: SchemaAction,
    },
    /// Manage tuples
    Tuple {
        #[command(subcommand)]
        action: TupleAction,
    },
    /// Manage filters
    Filter {
        #[command(subcommand)]
        action: FilterAction,
    },
    /// Manage playbooks
    Playbook {
        #[command(subcommand)]
        action: PlaybookAction,
    },
    /// Watch trigger events
    Trigger {
        #[command(subcommand)]
        action: TriggerAction,
    },
}

#[derive(Subcommand)]
enum SchemaAction {
    /// Register a schema from a JSON file
    Register {
        /// Schema name
        name: String,
        /// Path to JSON Schema file
        file: std::path::PathBuf,
    },
    /// Get a schema by name
    Get {
        /// Schema name
        name: String,
    },
    /// List all registered schemas
    List,
}

#[derive(Subcommand)]
enum FilterAction {
    /// Register a filter from a JSON file
    Register {
        /// Path to JSON filter definition file
        file: std::path::PathBuf,
    },
    /// List all registered filters
    List,
    /// Test a JSON object against all registered filters
    Test {
        /// Path to JSON file to match against filters
        file: std::path::PathBuf,
    },
}

#[derive(Subcommand)]
enum TupleAction {
    /// Put a tuple (reads data from a JSON file)
    Put {
        /// Schema type name
        tuple_type: String,
        /// Trace ID for correlating related tuples
        trace_id: String,
        /// Path to JSON file containing the tuple data
        file: std::path::PathBuf,
    },
    /// Get a tuple by UUID
    Get {
        /// UUID v7 of the tuple
        uuid7: String,
    },
}

#[derive(Subcommand)]
enum PlaybookAction {
    /// Register a playbook from a JSON file
    Register {
        /// Path to JSON playbook definition file
        file: std::path::PathBuf,
    },
    /// Get a playbook by name
    Get {
        /// Playbook name
        name: String,
    },
    /// List all registered playbooks
    List,
}

#[derive(Subcommand)]
enum TriggerAction {
    /// Stream trigger-fired events until ^C
    Watch {
        /// Filter to a specific playbook; omit for all playbooks
        #[arg(long)]
        playbook: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Version => {
            let mut client = TuplesClient::connect(SERVER_ADDR).await?;
            let response = client.get_version(Empty {}).await?;
            println!("{}", response.into_inner().version);
        }

        Command::Schema { action } => {
            let mut client = TuplesClient::connect(SERVER_ADDR).await?;
            match action {
                SchemaAction::Register { name, file } => {
                    let definition = std::fs::read_to_string(&file)
                        .with_context(|| format!("failed to read {}", file.display()))?;
                    serde_json::from_str::<serde_json::Value>(&definition)
                        .with_context(|| format!("{} is not valid JSON", file.display()))?;
                    client
                        .register_schema(RegisterSchemaRequest { name: name.clone(), definition })
                        .await?;
                    println!("registered schema '{name}'");
                }
                SchemaAction::Get { name } => {
                    let resp = client
                        .get_schema(GetSchemaRequest { name })
                        .await?
                        .into_inner();
                    let pretty = serde_json::from_str::<serde_json::Value>(&resp.definition)
                        .map(|v| serde_json::to_string_pretty(&v).unwrap())
                        .unwrap_or(resp.definition);
                    println!("{pretty}");
                }
                SchemaAction::List => {
                    let resp = client.list_schemas(Empty {}).await?.into_inner();
                    if resp.schemas.is_empty() {
                        println!("(no schemas registered)");
                    } else {
                        for s in resp.schemas {
                            println!("{}", s.name);
                        }
                    }
                }
            }
        }

        Command::Filter { action } => {
            let mut client = TuplesClient::connect(SERVER_ADDR).await?;
            match action {
                FilterAction::Register { file } => {
                    let definition = std::fs::read_to_string(&file)
                        .with_context(|| format!("failed to read {}", file.display()))?;
                    serde_json::from_str::<serde_json::Value>(&definition)
                        .with_context(|| format!("{} is not valid JSON", file.display()))?;
                    client
                        .register_filter(RegisterFilterRequest { definition })
                        .await?;
                    println!("filter registered");
                }
                FilterAction::List => {
                    let resp = client.list_filters(Empty {}).await?.into_inner();
                    if resp.filters.is_empty() {
                        println!("(no filters registered)");
                    } else {
                        for f in resp.filters {
                            println!("{}", f.id);
                        }
                    }
                }
                FilterAction::Test { file } => {
                    let data = std::fs::read_to_string(&file)
                        .with_context(|| format!("failed to read {}", file.display()))?;
                    serde_json::from_str::<serde_json::Value>(&data)
                        .with_context(|| format!("{} is not valid JSON", file.display()))?;
                    let resp =
                        client.match_tuple(MatchTupleRequest { data }).await?.into_inner();
                    if resp.filter_ids.is_empty() {
                        println!("(no filters matched)");
                    } else {
                        for id in resp.filter_ids {
                            println!("{id}");
                        }
                    }
                }
            }
        }

        Command::Tuple { action } => {
            let mut client = TuplesClient::connect(SERVER_ADDR).await?;
            match action {
                TupleAction::Put { tuple_type, trace_id, file } => {
                    let data = std::fs::read_to_string(&file)
                        .with_context(|| format!("failed to read {}", file.display()))?;
                    serde_json::from_str::<serde_json::Value>(&data)
                        .with_context(|| format!("{} is not valid JSON", file.display()))?;
                    let resp = client
                        .put_tuple(PutTupleRequest {
                            r#type: tuple_type,
                            trace_id,
                            data,
                            guaranteed_write: true,
                        })
                        .await?
                        .into_inner();
                    println!("uuid7:      {}", resp.uuid7);
                    println!("created_at: {}", resp.created_at);
                }
                TupleAction::Get { uuid7 } => {
                    let resp =
                        client.get_tuple(GetTupleRequest { uuid7 }).await?.into_inner();
                    let data_pretty =
                        serde_json::from_str::<serde_json::Value>(&resp.data)
                            .map(|v| serde_json::to_string_pretty(&v).unwrap())
                            .unwrap_or(resp.data);
                    println!("uuid7:      {}", resp.uuid7);
                    println!("type:       {}", resp.r#type);
                    println!("trace_id:   {}", resp.trace_id);
                    println!("created_at: {}", resp.created_at);
                    println!("data:       {data_pretty}");
                }
            }
        }

        Command::Playbook { action } => {
            let mut client = TuplesClient::connect(SERVER_ADDR).await?;
            match action {
                PlaybookAction::Register { file } => {
                    let definition = std::fs::read_to_string(&file)
                        .with_context(|| format!("failed to read {}", file.display()))?;
                    serde_json::from_str::<serde_json::Value>(&definition)
                        .with_context(|| format!("{} is not valid JSON", file.display()))?;
                    client
                        .register_playbook(RegisterPlaybookRequest { definition })
                        .await?;
                    println!("playbook registered");
                }
                PlaybookAction::Get { name } => {
                    let resp = client
                        .get_playbook(GetPlaybookRequest { name })
                        .await?
                        .into_inner();
                    let pretty = serde_json::from_str::<serde_json::Value>(&resp.definition)
                        .map(|v| serde_json::to_string_pretty(&v).unwrap())
                        .unwrap_or(resp.definition);
                    println!("{pretty}");
                }
                PlaybookAction::List => {
                    let resp = client.list_playbooks(Empty {}).await?.into_inner();
                    if resp.playbooks.is_empty() {
                        println!("(no playbooks registered)");
                    } else {
                        for p in resp.playbooks {
                            println!("{}", p.name);
                        }
                    }
                }
            }
        }

        Command::Trigger { action } => {
            let mut client = TuplesClient::connect(SERVER_ADDR).await?;
            match action {
                TriggerAction::Watch { playbook } => {
                    let req = WatchTriggersRequest {
                        playbook: playbook.unwrap_or_default(),
                    };
                    let mut stream = client.watch_triggers(req).await?.into_inner();
                    println!("watching for trigger events (^C to stop)...");
                    loop {
                        tokio::select! {
                            msg = stream.message() => {
                                match msg? {
                                    Some(event) => {
                                        println!(
                                            "trigger: playbook={} agent={} uuid7={} type={} params={:?}",
                                            event.playbook,
                                            event.agent,
                                            event.tuple_uuid7,
                                            event.tuple_type,
                                            event.mapped_params,
                                        );
                                    }
                                    None => {
                                        println!("stream closed by server");
                                        break;
                                    }
                                }
                            }
                            _ = tokio::signal::ctrl_c() => {
                                println!("\nstopped");
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
