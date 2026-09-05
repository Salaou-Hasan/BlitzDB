//! Sandboxed WASM job execution (Wasmtime), OFF the request hot path.
//!
//! Guest contract (WIT-less, deliberately tiny): the module exports a
//! linear `memory` and `run(input_ptr: i32, input_len: i32) -> i64` where
//! the result packs `(out_ptr << 32) | out_len`. Input is written at
//! offset 0 (memory grown as needed, capped); output is read back as
//! UTF-8. No host imports in v1: guests are pure compute over their input
//! (deterministic, auditable). Fuel metering bounds every call (runaways
//! die with [`JobError::FuelExhausted`], never wedging a worker); memory
//! is capped per store.
//!
//! Run guests on a blocking pool (`spawn_blocking` / dedicated threads):
//! compilation is cached per [`WasmExecutor`] (one `Engine`), but a call
//! still costs ~10-100µs plus guest time — never inline in dispatch.
//! TCP submission/status ops are future work; today this drives
//! in-process jobs (see [`run_job`]).

use wasmtime::{Config, Engine, Linker, Memory, Module, Store, StoreLimitsBuilder};

use crate::error::{JobError, JobResult};
use crate::job::Job;

/// Default fuel per call: generous for data shuffling, fatal for infinite
/// loops within milliseconds (fuel burns per instruction).
pub const DEFAULT_FUEL: u64 = 10_000_000;
/// Default per-store linear memory cap (64 pages = 4MiB).
pub const DEFAULT_MEMORY_MAX: u64 = 4 * 1024 * 1024;
/// Guest input base offset (inputs < 32KiB need no growth in practice).
pub const INPUT_BASE: usize = 0;
/// Guest output base offset (kept clear of the input window).
pub const OUTPUT_BASE: usize = 32 * 1024;

/// Compiled-engine holder: cheap to clone (`Engine` is `Arc`-backed),
/// expensive to build — construct once per process.
#[derive(Clone)]
pub struct WasmExecutor {
    engine: Engine,
    fuel: u64,
    memory_max: u64,
}

impl WasmExecutor {
    pub fn new() -> JobResult<Self> {
        Self::with_limits(DEFAULT_FUEL, DEFAULT_MEMORY_MAX)
    }

    pub fn with_limits(fuel: u64, memory_max: u64) -> JobResult<Self> {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).map_err(|e| JobError::Internal(e.to_string()))?;
        Ok(Self { engine, fuel, memory_max })
    }

    /// Precompile a module (fail fast on untrusted bytes, once).
    pub fn compile(&self, wasm: &[u8]) -> JobResult<Module> {
        Module::new(&self.engine, wasm).map_err(|e| JobError::BadModule(format!("{:#}", e)))
    }

    /// Run a compiled module's `run` over `input`, returning its UTF-8 output.
    pub fn run(&self, module: &Module, input: &str) -> JobResult<String> {
        let mut linker = Linker::new(&self.engine);
        let mut store = self.store()?;
        let instance = linker
            .instantiate(&mut store, module)
            .map_err(|e| JobError::BadModule(format!("instantiate: {}", e)))?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| JobError::BadSignature("missing exported memory \"memory\"".into()))?;
        let run = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, "run")
            .map_err(|_| {
                JobError::BadSignature("missing exported run(i32,i32)->i64 \"run\"".into())
            })?;
        let input_bytes = input.as_bytes();
        self.write_input(&mut store, &memory, input_bytes)?;
        let packed = run
            .call(&mut store, (INPUT_BASE as i32, input_bytes.len() as i32))
            .map_err(|e| self.trap(e))?;
        let out_ptr = (packed >> 32) as usize;
        let out_len = (packed & 0xFFFF_FFFF) as usize;
        self.read_output(&mut store, &memory, out_ptr, out_len)
    }

    /// Compile + run in one shot (convenience; prefer [`Self::compile`]
    /// once + [`Self::run`] per call for hot modules).
    pub fn run_bytes(&self, wasm: &[u8], input: &str) -> JobResult<String> {
        let module = self.compile(wasm)?;
        self.run(&module, input)
    }

    fn store(&self) -> JobResult<Store<wasmtime::StoreLimits>> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.memory_max as usize)
            .build();
        let mut store = Store::new(&self.engine, limits);
        store.limiter(|s| s as &mut wasmtime::StoreLimits);
        store
            .set_fuel(self.fuel)
            .map_err(|e| JobError::Internal(format!("fuel: {}", e)))?;
        Ok(store)
    }

    fn write_input(
        &self,
        store: &mut Store<wasmtime::StoreLimits>,
        memory: &Memory,
        input: &[u8],
    ) -> JobResult<()> {
        let end = INPUT_BASE + input.len();
        self.ensure(store, memory, end)?;
        memory
            .write(&mut *store, INPUT_BASE, input)
            .map_err(|e| JobError::GuestTrap(format!("input write: {}", e)))?;
        Ok(())
    }

    fn read_output(
        &self,
        store: &mut Store<wasmtime::StoreLimits>,
        memory: &Memory,
        ptr: usize,
        len: usize,
    ) -> JobResult<String> {
        // Bound single-read size by the memory cap (guest-controlled length
        // must not allocate unbounded host buffers).
        if len > self.memory_max as usize {
            return Err(JobError::GuestTrap(format!("output too large: {}", len)));
        }
        self.ensure(store, memory, ptr.saturating_add(len))?;
        let mut buf = vec![0u8; len];
        memory
            .read(&mut *store, ptr, &mut buf)
            .map_err(|e| JobError::GuestTrap(format!("output read: {}", e)))?;
        String::from_utf8(buf).map_err(|e| JobError::GuestTrap(format!("output not UTF-8: {}", e)))
    }

    fn ensure(
        &self,
        store: &mut Store<wasmtime::StoreLimits>,
        memory: &Memory,
        end: usize,
    ) -> JobResult<()> {
        let have = memory.data_size(&*store);
        if end <= have {
            return Ok(());
        }
        const PAGE: usize = 65536;
        let need = end.div_ceil(PAGE) - have.div_ceil(PAGE);
        memory
            .grow(&mut *store, need as u64)
            .map_err(|e| JobError::GuestTrap(format!("memory grow: {}", e)))?;
        Ok(())
    }

    fn trap(&self, err: wasmtime::Error) -> JobError {
        // Fuel exhaustion surfaces as a trap; the telling text lives in the
        // cause chain ("all fuel consumed..."), not the top message.
        let mut chain = format!("{:#}", err).to_lowercase();
        chain.push_str(&err.to_string().to_lowercase());
        if chain.contains("fuel") || chain.contains("out of gas") {
            JobError::FuelExhausted(self.fuel)
        } else {
            JobError::GuestTrap(format!("{:#}", err))
        }
    }
}

impl Default for WasmExecutor {
    fn default() -> Self {
        Self::new().expect("wasmtime engine builds")
    }
}

/// Run one [`Job`] through a WASM module, driving its lifecycle
/// (running/completed/failed). The job's `result` carries guest output.
/// Pure in-process in v1 (blocking-pool callers); TCP submit/status later.
pub fn run_job(executor: &WasmExecutor, job: &mut Job, wasm: &[u8], input: &str) -> JobResult<()> {
    job.mark_running();
    match executor.run_bytes(wasm, input) {
        Ok(out) => {
            job.mark_completed(out);
            Ok(())
        }
        Err(e) => {
            job.mark_failed(e.to_string());
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ECHO_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "run") (param $in i32) (param $len i32) (result i64)
    (local $i i32)
    (block $done
      (loop $cp
        (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
        (i32.store8
          (i32.add (i32.const 32768) (local.get $i))
          (i32.load8_u (i32.add (local.get $in) (local.get $i))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $cp)))
    (i64.or
      (i64.shl (i64.const 32768) (i64.const 32))
      (i64.extend_i32_u (local.get $len)))))"#;

    const LOOP_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "run") (param i32 i32) (result i64)
    (loop $l (br 0))
    (i64.const 0)))"#;

    const TRAP_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "run") (param i32 i32) (result i64)
    unreachable))"#;

    fn parse(wat: &str) -> Vec<u8> {
        wat::parse_str(wat).expect("test wat parses")
    }

    #[test]
    fn echo_roundtrip() {
        let ex = WasmExecutor::new().unwrap();
        let module = ex.compile(&parse(ECHO_WAT)).unwrap();
        // Compile once, run twice (engine/module reuse is the hot shape).
        for input in ["hello", "a longer input with spaces 123"] {
            let out = ex.run(&module, input).unwrap();
            assert_eq!(out, input);
        }
    }

    #[test]
    fn infinite_loop_dies_on_fuel() {
        let ex = WasmExecutor::with_limits(10_000, DEFAULT_MEMORY_MAX).unwrap();
        let err = ex.run_bytes(&parse(LOOP_WAT), "").unwrap_err();
        assert!(matches!(err, JobError::FuelExhausted(_)), "got {:?}", err);
    }

    #[test]
    fn trap_surfaces() {
        let ex = WasmExecutor::new().unwrap();
        let err = ex.run_bytes(&parse(TRAP_WAT), "").unwrap_err();
        assert!(matches!(err, JobError::GuestTrap(_)), "got {:?}", err);
    }

    #[test]
    fn missing_memory_rejected() {
        let ex = WasmExecutor::new().unwrap();
        let wat = r#"(module (func (export "run") (param i32 i32) (result i64) (i64.const 0)))"#;
        let err = ex.run_bytes(&parse(wat), "").unwrap_err();
        assert!(matches!(err, JobError::BadSignature(_)), "got {:?}", err);
    }

    #[test]
    fn bad_signature_rejected() {
        let ex = WasmExecutor::new().unwrap();
        let wat = r#"(module (memory (export "memory") 1) (func (export "run") (result i32) (i32.const 1)))"#;
        let err = ex.run_bytes(&parse(wat), "").unwrap_err();
        assert!(matches!(err, JobError::BadSignature(_)), "got {:?}", err);
    }

    #[test]
    fn garbage_bytes_rejected() {
        let ex = WasmExecutor::new().unwrap();
        let err = ex.run_bytes(b"\x00asm not wasm at all", "").unwrap_err();
        assert!(matches!(err, JobError::BadModule(_)), "got {:?}", err);
    }

    #[test]
    fn job_lifecycle_driven() {
        use crate::job::Job;
        let ex = WasmExecutor::new().unwrap();
        let module = parse(ECHO_WAT);
        let mut job = Job::new("echo");
        run_job(&ex, &mut job, &module, "payload").unwrap();
        assert_eq!(job.result.as_deref(), Some("payload"));
        let mut bad = Job::new("loop");
        let err = run_job(
            &WasmExecutor::with_limits(1_000, DEFAULT_MEMORY_MAX).unwrap(),
            &mut bad,
            &parse(LOOP_WAT),
            "",
        )
        .unwrap_err();
        assert!(matches!(err, JobError::FuelExhausted(_)));
        assert!(bad.error.is_some());
    }
}
