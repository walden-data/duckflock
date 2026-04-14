use std::collections::HashMap;

/// Represents an authenticated user/session identity.
///
/// The `metadata` map is intentionally open-ended — integrators (like Walden)
/// can store additional claims (tenant_id, roles, permissions) without
/// DuckFlock needing to know about them.
#[derive(Debug, Clone)]
pub struct Identity {
    pub username: String,
    pub metadata: HashMap<String, String>,
}

impl Identity {
    pub fn new(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}
