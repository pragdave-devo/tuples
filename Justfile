default:
  just --list

# Backend aliases
_features := ""
_mem       := ""
_fdb       := "fdb"
_pg        := "postgres"
_dynamo    := "dynamodb"

# Run tuplesd with the given backend features (empty = in-memory)
[no-exit-message]
run features=_mem label="" *args="":
    cargo run -p server {{ if features != "" { "--features " + features } else { "" } }} -- {{ args }}

# Build an optimized release of tuplesd (named tuplesd-<label> when a backend is selected)
release features=_mem label="":
    cargo build -p server --release {{ if features != "" { "--features " + features } else { "" } }}
    @if [ -n "{{ label }}" ]; then cp target/release/tuplesd target/release/tuplesd-{{ label }} && echo "target/release/tuplesd-{{ label }}"; else echo "target/release/tuplesd"; fi

# Shortcuts: just <backend> <run|release> [args...]
# e.g.  just fdb run --port 9090
#       just pg release
#       just dynamo run

[no-exit-message]
mem cmd *args="":
    just {{ cmd }} "{{ _mem }}" "" {{ args }}

[no-exit-message]
fdb cmd *args="":
    just {{ cmd }} "{{ _fdb }}" "fdb" {{ args }}

[no-exit-message]
pg cmd *args="":
    just {{ cmd }} "{{ _pg }}" "pg" {{ args }}

[no-exit-message]
dynamo cmd *args="":
    just {{ cmd }} "{{ _dynamo }}" "dynamo" {{ args }}

# Run the latest version of FoundationDB in a container
fdb-container:
    docker run "foundationdb/foundationdb@sha256:3987797a769bb5a8d6ee79378baea5a4bb3cb78a34018cd3b5b21711f42077a7"

# Generate API docs from proto files (requires: brew install protoc-gen-doc and pip install sabledocs)
[working-directory: 'proto']
docs:
    cp ../api-docs/_util/sabledocs.toml .
    protoc *.proto -o descriptor.pb --include_source_info
    sabledocs
    rm descriptor.pb sabledocs.toml

# Run the benchmark (release build) — forwards all args to tuples-bench
[no-exit-message]
bench-release *args="":
    cargo run --bin tuples-bench --release -- {{ args }}

# Rebuild docs and trigger a GitHub Pages deployment
pages: docs
    @echo "Docs regenerated in api-docs/. Push to main to deploy via GitHub Actions."
    @echo "Or run: gh workflow run docs.yml to deploy manually."

