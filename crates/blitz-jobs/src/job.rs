use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobPriority {
    Low,
    Normal,
    High,
    Critical,
}

impl JobPriority {
    pub fn order(&self) -> u8 {
        match self {
            JobPriority::Low => 0,
            JobPriority::Normal => 1,
            JobPriority::High => 2,
            JobPriority::Critical => 3,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: Uuid,
    pub job_type: String,
    pub status: JobStatus,
    pub priority: JobPriority,
    pub payload: HashMap<String, String>,
    pub result: Option<String>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub retry_count: u32,
    pub max_retries: u32,
}

impl Job {
    pub fn new(job_type: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            job_type: job_type.into(),
            status: JobStatus::Pending,
            priority: JobPriority::Normal,
            payload: HashMap::new(),
            result: None,
            error: None,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            retry_count: 0,
            max_retries: 3,
        }
    }

    pub fn with_priority(mut self, priority: JobPriority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_payload(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.payload.insert(key.into(), value.into());
        self
    }

    pub fn with_max_retries(mut self, max: u32) -> Self {
        self.max_retries = max;
        self
    }

    pub fn mark_running(&mut self) {
        self.status = JobStatus::Running;
        self.started_at = Some(Utc::now());
    }

    pub fn mark_completed(&mut self, result: impl Into<String>) {
        self.status = JobStatus::Completed;
        self.result = Some(result.into());
        self.completed_at = Some(Utc::now());
    }

    pub fn mark_failed(&mut self, error: impl Into<String>) {
        self.error = Some(error.into());
        if self.retry_count < self.max_retries {
            self.retry_count += 1;
            self.status = JobStatus::Pending;
        } else {
            self.status = JobStatus::Failed;
            self.completed_at = Some(Utc::now());
        }
    }

    pub fn mark_cancelled(&mut self) {
        self.status = JobStatus::Cancelled;
        self.completed_at = Some(Utc::now());
    }

    pub fn can_retry(&self) -> bool {
        self.status == JobStatus::Pending && self.retry_count < self.max_retries
    }

    pub fn duration_ms(&self) -> Option<f64> {
        match (self.started_at, self.completed_at) {
            (Some(start), Some(end)) => {
                Some((end - start).num_milliseconds() as f64)
            }
            _ => None,
        }
    }
}

/// A handler function for a job type.
pub type JobHandler = Box<dyn Fn(&mut Job) + Send + Sync>;

/// Background job scheduler.
pub struct JobScheduler {
    jobs: Arc<Mutex<HashMap<Uuid, Job>>>,
    handlers: Arc<Mutex<HashMap<String, JobHandler>>>,
}

impl JobScheduler {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(Mutex::new(HashMap::new())),
            handlers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a handler for a job type.
    pub fn register_handler(&self, job_type: impl Into<String>, handler: JobHandler) {
        self.handlers.lock().unwrap().insert(job_type.into(), handler);
    }

    /// Submit a job for execution.
    pub fn submit(&self, job: Job) -> Uuid {
        let id = job.id;
        self.jobs.lock().unwrap().insert(id, job);
        id
    }

    /// Get a job by ID.
    pub fn get_job(&self, id: Uuid) -> Option<Job> {
        self.jobs.lock().unwrap().get(&id).cloned()
    }

    /// Get all pending jobs, sorted by priority.
    pub fn pending_jobs(&self) -> Vec<Job> {
        let jobs = self.jobs.lock().unwrap();
        let mut pending: Vec<Job> = jobs
            .values()
            .filter(|j| j.status == JobStatus::Pending)
            .cloned()
            .collect();
        pending.sort_by(|a, b| b.priority.order().cmp(&a.priority.order()));
        pending
    }

    /// Get all jobs.
    pub fn all_jobs(&self) -> Vec<Job> {
        self.jobs.lock().unwrap().values().cloned().collect()
    }

    /// Get jobs by status.
    pub fn jobs_by_status(&self, status: &JobStatus) -> Vec<Job> {
        self.jobs
            .lock()
            .unwrap()
            .values()
            .filter(|j| j.status == *status)
            .cloned()
            .collect()
    }

    /// Get jobs by type.
    pub fn jobs_by_type(&self, job_type: &str) -> Vec<Job> {
        self.jobs
            .lock()
            .unwrap()
            .values()
            .filter(|j| j.job_type == job_type)
            .cloned()
            .collect()
    }

    /// Execute the next pending job.
    pub fn execute_next(&self) -> Option<Job> {
        let next = self.pending_jobs().into_iter().next()?;
        self.execute_job(next.id)
    }

    /// Execute a specific job by ID.
    pub fn execute_job(&self, id: Uuid) -> Option<Job> {
        let mut jobs = self.jobs.lock().unwrap();
        let job = jobs.get_mut(&id)?;
        if job.status != JobStatus::Pending {
            return None;
        }
        job.mark_running();

        let handlers = self.handlers.lock().unwrap();
        if let Some(handler) = handlers.get(&job.job_type) {
            handler(job);
        } else {
            job.mark_completed("no handler registered");
        }

        Some(job.clone())
    }

    /// Cancel a job.
    pub fn cancel_job(&self, id: Uuid) -> bool {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&id) {
            if job.status == JobStatus::Pending || job.status == JobStatus::Running {
                job.mark_cancelled();
                return true;
            }
        }
        false
    }

    /// Get the number of jobs.
    pub fn job_count(&self) -> usize {
        self.jobs.lock().unwrap().len()
    }

    /// Get the number of pending jobs.
    pub fn pending_count(&self) -> usize {
        self.jobs
            .lock()
            .unwrap()
            .values()
            .filter(|j| j.status == JobStatus::Pending)
            .count()
    }
}

impl Default for JobScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_job_creation() {
        let job = Job::new("send_email");
        assert_eq!(job.job_type, "send_email");
        assert_eq!(job.status, JobStatus::Pending);
        assert_eq!(job.priority, JobPriority::Normal);
        assert_eq!(job.retry_count, 0);
        assert_eq!(job.max_retries, 3);
    }

    #[test]
    fn test_job_builder() {
        let job = Job::new("deploy")
            .with_priority(JobPriority::High)
            .with_payload("env", "production")
            .with_max_retries(5);
        assert_eq!(job.priority, JobPriority::High);
        assert_eq!(job.payload.get("env").unwrap(), "production");
        assert_eq!(job.max_retries, 5);
    }

    #[test]
    fn test_job_lifecycle() {
        let mut job = Job::new("test");
        assert_eq!(job.status, JobStatus::Pending);

        job.mark_running();
        assert_eq!(job.status, JobStatus::Running);
        assert!(job.started_at.is_some());

        job.mark_completed("done");
        assert_eq!(job.status, JobStatus::Completed);
        assert_eq!(job.result.as_deref(), Some("done"));
        assert!(job.completed_at.is_some());
        assert!(job.duration_ms().is_some());
    }

    #[test]
    fn test_job_failure_retry() {
        let mut job = Job::new("test").with_max_retries(2);
        job.mark_running();
        job.mark_failed("error 1");
        assert_eq!(job.status, JobStatus::Pending); // retries
        assert_eq!(job.retry_count, 1);

        job.mark_running();
        job.mark_failed("error 2");
        assert_eq!(job.status, JobStatus::Pending);
        assert_eq!(job.retry_count, 2);

        job.mark_running();
        job.mark_failed("error 3");
        assert_eq!(job.status, JobStatus::Failed);
        assert_eq!(job.retry_count, 2);
    }

    #[test]
    fn test_scheduler_submit_and_get() {
        let scheduler = JobScheduler::new();
        let id = scheduler.submit(Job::new("test"));
        assert!(scheduler.get_job(id).is_some());
        assert_eq!(scheduler.job_count(), 1);
    }

    #[test]
    fn test_scheduler_execute() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let scheduler = JobScheduler::new();
        scheduler.register_handler(
            "increment",
            Box::new(move |job| {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                job.mark_completed("done");
            }),
        );

        let id = scheduler.submit(Job::new("increment"));
        let result = scheduler.execute_job(id).unwrap();
        assert_eq!(result.status, JobStatus::Completed);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_scheduler_priority_ordering() {
        let scheduler = JobScheduler::new();
        scheduler.submit(Job::new("low").with_priority(JobPriority::Low));
        scheduler.submit(Job::new("critical").with_priority(JobPriority::Critical));
        scheduler.submit(Job::new("normal").with_priority(JobPriority::Normal));

        let pending = scheduler.pending_jobs();
        assert_eq!(pending[0].job_type, "critical");
        assert_eq!(pending[1].job_type, "normal");
        assert_eq!(pending[2].job_type, "low");
    }

    #[test]
    fn test_scheduler_cancel() {
        let scheduler = JobScheduler::new();
        let id = scheduler.submit(Job::new("test"));
        assert!(scheduler.cancel_job(id));
        let job = scheduler.get_job(id).unwrap();
        assert_eq!(job.status, JobStatus::Cancelled);
    }

    #[test]
    fn test_scheduler_execute_next() {
        let scheduler = JobScheduler::new();
        scheduler.register_handler("test", Box::new(|job| job.mark_completed("ok")));
        scheduler.submit(Job::new("test").with_priority(JobPriority::Normal));
        scheduler.submit(Job::new("test").with_priority(JobPriority::High));

        let result = scheduler.execute_next().unwrap();
        assert_eq!(result.priority, JobPriority::High);
    }

    #[test]
    fn test_jobs_by_type() {
        let scheduler = JobScheduler::new();
        scheduler.submit(Job::new("email"));
        scheduler.submit(Job::new("email"));
        scheduler.submit(Job::new("deploy"));

        let emails = scheduler.jobs_by_type("email");
        assert_eq!(emails.len(), 2);
    }
}
