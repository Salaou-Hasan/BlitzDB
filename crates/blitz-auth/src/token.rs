use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct AccessToken {
    pub token: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct RefreshToken {
    pub token: String,
    pub expires_at: DateTime<Utc>,
}

impl AccessToken {
    pub fn new(token: impl Into<String>, expires_at: DateTime<Utc>) -> Self {
        Self {
            token: token.into(),
            expires_at,
        }
    }

    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }
}

impl RefreshToken {
    pub fn new(token: impl Into<String>, expires_at: DateTime<Utc>) -> Self {
        Self {
            token: token.into(),
            expires_at,
        }
    }

    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }
}
