use uuid::Uuid;

pub struct SubscriptionManager;

impl SubscriptionManager {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}
