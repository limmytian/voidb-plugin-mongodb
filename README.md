# VoidB MongoDB Plugin (`voidb-plugin-mongodb`)

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

External process plugin for [VoidB](https://github.com/limmytian/voidb), providing comprehensive MongoDB database, collection, document management, and aggregation capabilities.

## Architecture

This plugin runs as an autonomous, out-of-process plugin adhering to VoidB's `process-plugin-sdk` stdio-jsonrpc protocol.

## Features

- **Autonomous Process Architecture**: Runs in an isolated OS process communicating with VoidB via `stdio-jsonrpc`.
- **Tri-Modal Operation**:
  - `test`: Automated connectivity verification.
  - `serve`: `stdio-jsonrpc` protocol server for AI Agents and automated orchestration.
  - Domain commands: Rich database catalog, document queries, index management, and aggregation commands.
- **Capabilities Exposed**:
  - `diagnostics`: Agent-safe profile diagnostics without opening live cluster connections.
  - `databases`: Database catalog listing.
  - `collections`: Collection catalog listing per database.
  - `find`: Filtered, sorted document querying with cursor pagination.
  - `count`: Document count matching query filters.
  - `aggregate`: Read-only aggregation pipeline execution.
  - `cursor_read`: Streaming consumption of persistent find or aggregation cursors.
  - `change_stream_read`: Streaming collection change stream watcher.
  - `indexes`: Index catalog inspection.
  - `insert`: Single document insertion.
  - `update`: Filtered document update.
  - `delete`: Filtered document deletion.
  - `bulk_write`: Ordered and unordered batch write operations.

## Usage & Operational Modes

When installed, `voidb-cli` automatically discovers this plugin and provides seamless transparent dispatch:

### 1. Human Operations (CLI)

```bash
# Connectivity test
voidb-cli mongodb test --profile local-mongo

# Database and collection discovery
voidb-cli mongodb dbs --profile local-mongo
voidb-cli mongodb collections my_db --profile local-mongo
voidb-cli mongodb stats my_db my_coll --profile local-mongo

# Document operations and queries
voidb-cli mongodb find my_db my_coll --filter '{"status":"active"}' --limit 10 --profile local-mongo
voidb-cli mongodb count my_db my_coll --filter '{"status":"active"}' --profile local-mongo
voidb-cli mongodb indexes my_db my_coll --profile local-mongo
voidb-cli mongodb aggregate my_db my_coll --pipeline '[{"$match":{"status":"active"}},{"$count":"total"}]' --profile local-mongo
```

### 2. Machine Communication (AI Agents & JSON-RPC)

```bash
# Run the stdio-jsonrpc server directly or via voidb-cli
voidb-cli mongodb serve
# Or directly run the binary:
./bin/voidb-plugin-mongodb serve
```

## Quick Start

### Installation

Place this plugin directory or a packaged release archive under your VoidB plugins directory (`~/Library/Application Support/voidb/plugins/mongodb` on macOS, or `~/.local/share/voidb/plugins/mongodb` on Linux):

```bash
mkdir -p "$HOME/Library/Application Support/voidb/plugins/mongodb"
cp -r plugin.toml bin schemas "$HOME/Library/Application Support/voidb/plugins/mongodb/"
```

Verify discovery via `voidb`:

```bash
voidb-cli plugin list
voidb-cli mongodb --help
```

### Development & Build

```bash
cargo build --release
mkdir -p bin
cp target/release/voidb-plugin-mongodb bin/
```

## Protocol Specifications

Complies with the [VoidB Process Plugin Protocol](https://github.com/limmytian/voidb/blob/main/docs/quickstart-process-plugin.md) specification (v1.0).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
