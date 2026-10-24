# agentlink

A LAN message bus for AI agents — this crate is the **open-source core** of the
[AgentLink](https://github.com/weifengchui/AgentLink) node: discovery, WebSocket
mesh transport, reliable delivery, file transfer, the local HTTP/WS API and the
live web console, all in one pure-Rust library with zero C dependencies.

The official `agentlink` CLI binary is a thin shell assembled on top of this
crate. Use the library directly to embed a node into your own binary, build a
custom distribution, or extend routing and delivery behavior.

```toml
[dependencies]
agentlink = "0.6"            # add features = ["mdns"] for mDNS discovery
```

See [`src/main.rs` in the repository](https://github.com/weifengchui/AgentLink/blob/main/src/main.rs)
for a complete, minimal assembly: config loading → router → peer connections →
local API server — one readable file, no magic.

## Module map

| Module | Responsibility |
|---|---|
| `api` | Local HTTP/WS API (`/v1/*`), peer WebSocket endpoints, web console & starmap |
| `router` | Message routing: ACK, bounded retry, offline queue (TTL), broadcast |
| `conn` | Outbound peer connections, dial-direction by `node_id` order |
| `peer` | Inbound peer handling, HMAC handshake, frame signing, replay window |
| `discovery` | UDP broadcast discovery; `mdns` feature adds mDNS; static peers always available |
| `file` | Chunked file transfer over a dedicated peer connection, SHA-256 verified, resumable |
| `config` | On-disk config schema (`agentlink.json`), identity, security modes |
| `store` | History ring buffer & persistence |
| `proto` | The JSON envelope shared by every message |
| `cli` | REST thin-shell subcommands (`get`, `logs`, `apply`, `hello`, …) |
| `wizard` | First-run interactive setup |
| `redact` / `trace` / `logging` | Observer-stream redaction, tracing glue, log setup |

## Extension points

The core is designed to be extended without forking:

- **`api::bridge::MeshBridge`** — project extra routes, state and behavior onto
  the API layer (used by the commercial edition for orchestration/RBAC/audit/HA).
- **`core_hook::CoreHook`** — observe router lifecycle events.
- **`extensions`** — a typed map carried by the shared app state, so downstream
  layers can attach their own state without touching core types.

Dependency direction is a hard rule: this crate never references closed-source
code; commercial capabilities mount in the reverse direction through the two
traits above.

## Feature flags

| Feature | Effect |
|---|---|
| `mdns` | mDNS/DNS-SD discovery alongside UDP broadcast |

## Notes

- Edition 2021, pure Rust, no C toolchain required.
- The "AgentLink" name is a trademark — see
  [`NOTICE`](https://github.com/weifengchui/AgentLink/blob/main/NOTICE).
- Licensed under Apache-2.0.
