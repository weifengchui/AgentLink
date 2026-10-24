// SPDX-License-Identifier: Apache-2.0
//! Optional mDNS discovery (`--features mdns`), per design doc §6.
//!
//! When enabled alongside `udp_broadcast` in `discovery.methods`, the node
//! advertises `_agentlink._tcp.local.` and discovers peers the same way.

use std::collections::HashMap;

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use tokio::sync::watch;

use crate::api::SharedState;
use crate::config::Config;
use crate::peer::DiscoveredPeer;

const SERVICE_TYPE: &str = "_agentlink._tcp.local.";

pub async fn run(cfg: Config, state: SharedState, mut shutdown: watch::Receiver<bool>) {
    let daemon = match ServiceDaemon::new() {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(error = %e, "mdns: failed to start daemon");
            return;
        }
    };

    // Advertise our service.
    let Some(ip) = local_ip_address::local_ip()
        .ok()
        .filter(|ip| !ip.is_loopback())
    else {
        tracing::warn!("mdns: no non-loopback address found");
        return;
    };
    let mut props: HashMap<String, String> = [
        ("node_id".to_string(), cfg.node_id.clone()),
        ("name".to_string(), state.display_name()),
    ]
    .into_iter()
    .collect();
    if cfg.mtls.enabled && state.bridge().is_some_and(|b| b.tls_ready()) {
        props.insert("mtls_port".to_string(), cfg.mtls_listen_port().to_string());
    }
    let instance = format!("{}#{}", cfg.name, &cfg.node_id[..4.min(cfg.node_id.len())]);
    match ServiceInfo::new(
        SERVICE_TYPE,
        &instance,
        &format!("{}.local.", cfg.node_id),
        ip,
        cfg.listen_port,
        props,
    ) {
        Ok(si) => {
            if let Err(e) = daemon.register(si) {
                tracing::warn!(error = %e, "mdns: register failed");
            } else {
                tracing::info!(ip = %ip, "mdns: advertising _agentlink._tcp.local.");
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "mdns: invalid service info");
        }
    }

    let receiver = match daemon.browse(SERVICE_TYPE) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "mdns: browse failed");
            return;
        }
    };

    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            ev = async { receiver.recv_async().await } => {
                if let Ok(ServiceEvent::ServiceResolved(info)) = ev {
                    handle_resolved(&cfg, &state, &info);
                }
            }
        }
    }
    let _ = daemon.shutdown();
}

fn handle_resolved(cfg: &Config, state: &SharedState, info: &ServiceInfo) {
    let Some(node_id) = info.get_property_val_str("node_id").map(|s| s.to_string()) else {
        return;
    };
    if node_id == cfg.node_id {
        return; // our own advertisement
    }
    let name = info
        .get_property_val_str("name")
        .unwrap_or_default()
        .to_string();
    let port = info.get_port();
    let ip: Option<std::net::Ipv4Addr> = info.get_addresses_v4().into_iter().next().copied();
    let Some(ip) = ip else {
        return;
    };

    let addr = format!("{ip}:{port}");
    let mtls_addr = info
        .get_property_val_str("mtls_port")
        .and_then(|s| s.parse::<u16>().ok())
        .map(|p| format!("{ip}:{p}"));
    let is_new =
        state
            .peers
            .upsert_discovered(&node_id, &name, &addr, mtls_addr.clone(), Vec::new());
    if is_new {
        tracing::info!(peer = %name, node_id = %node_id, addr, "mdns: peer discovered");
    }
    // Same channel as UDP discovery; the connection manager dedups and applies
    // the dial-direction rule.
    let _ = state.peer_tx.try_send(DiscoveredPeer {
        node_id,
        name,
        addr,
        mtls_addr,
    });
}
