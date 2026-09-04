use blitz_auth::{Identity, Permission};
use std::collections::HashSet;

/// Effect of a policy rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}

/// A policy rule: if subject matches + action matches + resource matches → effect.
#[derive(Debug, Clone)]
pub struct PolicyRule {
    pub roles: Vec<String>,
    pub resource: String,
    pub permission: Permission,
    pub effect: Effect,
}

impl PolicyRule {
    pub fn allow(
        roles: impl Into<Vec<String>>,
        resource: impl Into<String>,
        permission: Permission,
    ) -> Self {
        Self {
            roles: roles.into(),
            resource: resource.into(),
            permission,
            effect: Effect::Allow,
        }
    }

    pub fn deny(
        roles: impl Into<Vec<String>>,
        resource: impl Into<String>,
        permission: Permission,
    ) -> Self {
        Self {
            roles: roles.into(),
            resource: resource.into(),
            permission,
            effect: Effect::Deny,
        }
    }
}

/// Policy engine that evaluates rules against identities.
pub struct PolicyEngine {
    rules: Vec<PolicyRule>,
}

impl PolicyEngine {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn add_rule(&mut self, rule: PolicyRule) {
        self.rules.push(rule);
    }

    /// Check if an identity is allowed to perform an action on a resource.
    /// Returns Ok(true) if allowed, Ok(false) if denied, Err if no matching rule.
    pub fn check(
        &self,
        identity: &Identity,
        resource: &str,
        permission: &Permission,
    ) -> crate::PolicyResult<bool> {
        let mut matched_deny = false;
        let mut matched_allow = false;

        for rule in &self.rules {
            let resource_matches = rule.resource == "*" || rule.resource == resource;
            if !resource_matches {
                continue;
            }

            let permission_matches = rule.permission == *permission;
            if !permission_matches {
                continue;
            }

            let role_matches = rule.roles.iter().any(|r| identity.has_role(r));
            if !role_matches {
                continue;
            }

            match rule.effect {
                Effect::Deny => {
                    matched_deny = true;
                    break;
                }
                Effect::Allow => {
                    matched_allow = true;
                }
            }
        }

        if matched_deny {
            Ok(false)
        } else if matched_allow {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get all resources a given identity has a specific permission for.
    pub fn allowed_resources(
        &self,
        identity: &Identity,
        permission: &Permission,
    ) -> Vec<String> {
        let mut resources = HashSet::new();
        for rule in &self.rules {
            if rule.effect != Effect::Allow {
                continue;
            }
            if rule.permission != *permission {
                continue;
            }
            if !rule.roles.iter().any(|r| identity.has_role(r)) {
                continue;
            }
            if rule.resource == "*" {
                resources.insert("*".to_string());
            } else {
                resources.insert(rule.resource.clone());
            }
        }
        resources.into_iter().collect()
    }

    /// Get all permissions an identity has on a specific resource.
    pub fn permissions_for_resource(
        &self,
        identity: &Identity,
        resource: &str,
    ) -> Vec<Permission> {
        let mut perms = HashSet::new();
        for rule in &self.rules {
            if rule.effect != Effect::Allow {
                continue;
            }
            let resource_matches = rule.resource == "*" || rule.resource == resource;
            if !resource_matches {
                continue;
            }
            if !rule.roles.iter().any(|r| identity.has_role(r)) {
                continue;
            }
            perms.insert(rule.permission.clone());
        }
        perms.into_iter().collect()
    }
}

impl Default for PolicyEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin() -> Identity {
        Identity::new("admin").with_role("admin")
    }

    fn editor() -> Identity {
        Identity::new("editor").with_role("editor")
    }

    fn viewer() -> Identity {
        Identity::new("viewer").with_role("viewer")
    }

    #[test]
    fn test_basic_allow() {
        let mut engine = PolicyEngine::new();
        engine.add_rule(PolicyRule::allow(
            vec!["editor".into()],
            "posts",
            Permission::Write,
        ));

        let id = editor();
        assert!(engine.check(&id, "posts", &Permission::Write).unwrap());
        assert!(!engine.check(&id, "posts", &Permission::Delete).unwrap());
    }

    #[test]
    fn test_deny_overrides_allow() {
        let mut engine = PolicyEngine::new();
        engine.add_rule(PolicyRule::allow(
            vec!["editor".into()],
            "posts",
            Permission::Write,
        ));
        engine.add_rule(PolicyRule::deny(
            vec!["editor".into()],
            "posts",
            Permission::Write,
        ));

        let id = editor();
        assert!(!engine.check(&id, "posts", &Permission::Write).unwrap());
    }

    #[test]
    fn test_wildcard_resource() {
        let mut engine = PolicyEngine::new();
        engine.add_rule(PolicyRule::allow(
            vec!["viewer".into()],
            "*",
            Permission::Read,
        ));

        let id = viewer();
        assert!(engine.check(&id, "posts", &Permission::Read).unwrap());
        assert!(engine.check(&id, "users", &Permission::Read).unwrap());
        assert!(!engine.check(&id, "posts", &Permission::Write).unwrap());
    }

    #[test]
    fn test_admin_deny_takes_precedence() {
        let mut engine = PolicyEngine::new();
        engine.add_rule(PolicyRule::deny(
            vec!["admin".into()],
            "system",
            Permission::Custom("shutdown".into()),
        ));

        let id = admin();
        assert!(!engine
            .check(&id, "system", &Permission::Custom("shutdown".into()))
            .unwrap());
    }

    #[test]
    fn test_allowed_resources() {
        let mut engine = PolicyEngine::new();
        engine.add_rule(PolicyRule::allow(
            vec!["editor".into()],
            "posts",
            Permission::Write,
        ));
        engine.add_rule(PolicyRule::allow(
            vec!["editor".into()],
            "comments",
            Permission::Write,
        ));

        let id = editor();
        let resources = engine.allowed_resources(&id, &Permission::Write);
        assert_eq!(resources.len(), 2);
        assert!(resources.contains(&"posts".to_string()));
        assert!(resources.contains(&"comments".to_string()));
    }

    #[test]
    fn test_permissions_for_resource() {
        let mut engine = PolicyEngine::new();
        engine.add_rule(PolicyRule::allow(
            vec!["editor".into()],
            "posts",
            Permission::Read,
        ));
        engine.add_rule(PolicyRule::allow(
            vec!["editor".into()],
            "posts",
            Permission::Write,
        ));

        let id = editor();
        let perms = engine.permissions_for_resource(&id, "posts");
        assert_eq!(perms.len(), 2);
        assert!(perms.contains(&Permission::Read));
        assert!(perms.contains(&Permission::Write));
    }

    #[test]
    fn test_no_matching_rule_denies() {
        let engine = PolicyEngine::new();
        let id = viewer();
        assert!(!engine.check(&id, "posts", &Permission::Write).unwrap());
    }

    #[test]
    fn test_multiple_roles() {
        let mut engine = PolicyEngine::new();
        engine.add_rule(PolicyRule::allow(
            vec!["editor".into()],
            "posts",
            Permission::Write,
        ));

        let id = Identity::new("multi").with_role("editor").with_role("viewer");
        assert!(engine.check(&id, "posts", &Permission::Write).unwrap());
    }
}
