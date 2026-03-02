fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile_protos(
        &[
            "../../proto/common.proto",
            "../../proto/schemas.proto",
            "../../proto/tuples.proto",
            "../../proto/playbooks.proto",
            "../../proto/agents.proto",
            "../../proto/runs.proto",
        ],
        &["../../proto"],
    )?;
    Ok(())
}
