use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Job {
    pub id: Uuid,
    pub job_type: String,
    pub status: JobStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl Job {
    pub fn new(job_type: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            job_type: job_type.into(),
            status: JobStatus::Pending,
            created_at: Utc::now(),
        }
    }
}
