# VoidB MongoDB Plugin

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

External process plugin for [VoidB](https://github.com/limmytian/voidb), providing comprehensive MongoDB database, collection, document management, and aggregation capabilities.

## Architecture

This plugin runs as an autonomous, out-of-process plugin adhering to VoidB's `process-plugin-sdk` stdio-jsonrpc protocol.

### Capabilities Exposed

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
- `create_index`: Index creation.
- `run_command`: Gated raw database administrative command execution.

## Building and Installing

```bash
cargo build --release
mkdir -p bin
cp target/release/voidb-plugin-mongodb bin/
```

Then point VoidB to this directory via:
```bash
export VOIDB_PLUGIN_PATH=/path/to/voidb-plugin-mongodb
```

## Running Standalone

```bash
# Run the stdio-jsonrpc RPC server
./bin/voidb-plugin-mongodb serve
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
