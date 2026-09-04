use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Identity {
    pub id: Uuid,
    pub subject: String,
    pub roles: Vec<String>,
    pub claims: std::collections::HashMap<String, String>,
}

impl Identity {
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            subject: subject.into(),
            roles: Vec::new(),
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
}
