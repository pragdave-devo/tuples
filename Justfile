default:
  just --list

# Run the latest version of FoundationDB in a container
fdb:
    docker run "foundationdb/foundationdb@sha256:3987797a769bb5a8d6ee79378baea5a4bb3cb78a34018cd3b5b21711f42077a7"

# Generate API docs from proto files (requires: brew install protoc-gen-doc)
docs:
    mkdir -p docs
    protoc --doc_out=./docs --doc_opt=markdown,api.md -Iproto proto/*.proto

