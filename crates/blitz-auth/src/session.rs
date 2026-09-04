use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::identity::Identity;

#[derive(Debug, Clone)]
pub struct Session {
    pub id: Uuid,
    pub identity: Identity,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl Session {
    pub fn new(identity: Identity, expires_at: DateTime<Utc>) -> Self {
        Self {
            id: Uuid::new_v4(),
            identity,
            created_at: Utc::now(),
            expires_at,
        }
    }

    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }
}
