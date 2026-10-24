// SPDX-License-Identifier: Apache-2.0
//! Logging initialisation based on the node configuration.

use tracing_subscriber::EnvFilter;

use crate::config::LogConfig;

/// Initialise the global tracing subscriber.
///
/// `RUST_LOG` overrides the configured level when set.
pub fn init(cfg: &LogConfig) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(cfg.level.clone()));

    let builder = tracing_subscriber::fmt().with_env_filter(filter);

    match cfg.format.as_str() {
        "json" => {
            let _ = builder.json().try_init();
        }
        _ => {
            let _ = builder.try_init();
        }
    }
}
