use uuid::Uuid;

/// A permission that can be granted to an identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Permission {
    Read,
    Write,
    Delete,
    Admin,
    Custom(String),
}

impl Permission {
    pub fn as_str(&self) -> &str {
        match self {
            Permission::Read => "read",
            Permission::Write => "write",
            Permission::Delete => "delete",
            Permission::Admin => "admin",
            Permission::Custom(s) => s,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Identity {
    pub id: Uuid,
    pub subject: String,
    pub roles: Vec<String>,
    pub permissions: Vec<Permission>,
    pub claims: std::collections::HashMap<String, String>,
}

impl Identity {
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            subject: subject.into(),
            roles: Vec::new(),
            permissions: Vec::new(),
            claims: std::collections::HashMap::new(),
        }
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(&role.to_string())
    }

    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles.push(role.into());
        self
    }

    pub fn with_permission(mut self, perm: Permission) -> Self {
        self.permissions.push(perm);
        self
    }

    pub fn has_permission(&self, perm: &Permission) -> bool {
        if self.permissions.iter().any(|p| p == perm) {
            return true;
        }
        if self.has_role("admin") {
            return true;
        }
        false
    }

    pub fn with_claim(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.claims.insert(key.into(), value.into());
        self
    }

    pub fn get_claim(&self, key: &str) -> Option<&str> {
        self.claims.get(key).map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_creation() {
        let id = Identity::new("user1");
        assert_eq!(id.subject, "user1");
        assert!(id.roles.is_empty());
        assert!(id.permissions.is_empty());
    }

    #[test]
    fn test_identity_roles() {
        let id = Identity::new("admin").with_role("admin").with_role("editor");
        assert!(id.has_role("admin"));
        assert!(id.has_role("editor"));
        assert!(!id.has_role("viewer"));
    }

    #[test]
    fn test_identity_permissions() {
        let id = Identity::new("user1")
            .with_permission(Permission::Read)
            .with_permission(Permission::Write);
        assert!(id.has_permission(&Permission::Read));
        assert!(id.has_permission(&Permission::Write));
        assert!(!id.has_permission(&Permission::Delete));
    }

    #[test]
    fn test_admin_has_all_permissions() {
        let id = Identity::new("admin").with_role("admin");
        assert!(id.has_permission(&Permission::Read));
        assert!(id.has_permission(&Permission::Write));
        assert!(id.has_permission(&Permission::Delete));
        assert!(id.has_permission(&Permission::Admin));
    }

    #[test]
    fn test_identity_claims() {
        let id = Identity::new("user1")
            .with_claim("org", "acme")
            .with_claim("plan", "pro");
        assert_eq!(id.get_claim("org"), Some("acme"));
        assert_eq!(id.get_claim("plan"), Some("pro"));
        assert_eq!(id.get_claim("missing"), None);
    }

    #[test]
    fn test_custom_permission() {
        let id = Identity::new("user1").with_permission(Permission::Custom("deploy".into()));
        assert!(id.has_permission(&Permission::Custom("deploy".into())));
        assert!(!id.has_permission(&Permission::Custom("destroy".into())));
    }
}
