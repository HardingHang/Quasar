//! Lightweight metrics collection without external dependencies.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Simple atomic counter.
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

/// Application-level metrics registry.
pub struct MetricsRegistry {
    http_requests: Mutex<HashMap<(String, String, u16), Counter>>,
    iceberg_commit_conflicts: Counter,
    iceberg_commit_successes: Counter,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            http_requests: Mutex::new(HashMap::new()),
            iceberg_commit_conflicts: Counter::new(),
            iceberg_commit_successes: Counter::new(),
        }
    }

    pub fn record_http_request(&self, method: &str, path: &str, status: u16) {
        let key = (method.to_string(), path.to_string(), status);
        let mut map = self.http_requests.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(key).or_default().inc();
    }

    pub fn record_iceberg_commit_conflict(&self) {
        self.iceberg_commit_conflicts.inc();
    }

    pub fn record_iceberg_commit_success(&self) {
        self.iceberg_commit_successes.inc();
    }

    pub fn render(&self) -> String {
        let mut output = String::new();

        output.push_str("# HELP http_requests_total Total HTTP requests\n");
        output.push_str("# TYPE http_requests_total counter\n");
        {
            let map = self.http_requests.lock().unwrap_or_else(|e| e.into_inner());
            for ((method, path, status), counter) in map.iter() {
                output.push_str(&format!(
                    "http_requests_total{{method=\"{}\",path=\"{}\",status=\"{}\"}} {}\n",
                    method,
                    path,
                    status,
                    counter.get()
                ));
            }
        }

        output.push_str(
            "\n# HELP iceberg_commit_conflicts_total Total Iceberg commit CAS conflicts\n",
        );
        output.push_str("# TYPE iceberg_commit_conflicts_total counter\n");
        output.push_str(&format!(
            "iceberg_commit_conflicts_total {}\n",
            self.iceberg_commit_conflicts.get()
        ));

        output.push_str("\n# HELP iceberg_commit_successes_total Total Iceberg commit successes\n");
        output.push_str("# TYPE iceberg_commit_successes_total counter\n");
        output.push_str(&format!(
            "iceberg_commit_successes_total {}\n",
            self.iceberg_commit_successes.get()
        ));

        output
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared metrics state (cloneable via Arc).
#[derive(Clone)]
pub struct MetricsState {
    pub registry: std::sync::Arc<MetricsRegistry>,
}

impl Default for MetricsState {
    fn default() -> Self {
        Self {
            registry: std::sync::Arc::new(MetricsRegistry::new()),
        }
    }
}
