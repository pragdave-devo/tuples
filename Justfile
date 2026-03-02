default:
  just --list

# Run the latest version of FoundationDB in a container
fdb:
    docker run "foundationdb/foundationdb@sha256:3987797a769bb5a8d6ee79378baea5a4bb3cb78a34018cd3b5b21711f42077a7"

# Generate API docs from proto files (requires: brew install protoc-gen-doc and pip install sabledocs)
[working-directory: 'proto']
docs:
    cp ../api-docs/_util/sabledocs.toml .
    protoc *.proto -o descriptor.pb --include_source_info
    sabledocs
    rm descriptor.pb sabledocs.toml

# Rebuild docs and trigger a GitHub Pages deployment
pages: docs
    @echo "Docs regenerated in api-docs/. Push to main to deploy via GitHub Actions."
    @echo "Or run: gh workflow run docs.yml to deploy manually."

