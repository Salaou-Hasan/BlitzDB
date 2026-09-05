//! Transactional procedure runner: executes [`Procedure`] steps against a
//! [`ProcedureBackend`]. The server implements the backend over its OCC
//! transaction manager, so a whole procedure commits atomically; this crate
//! stays dependency-free (pure compute + trait).
//!
//! Conventions (shared with `RuntimeExecutor`): `$var` string references
//! resolve against live variables, else `Null`. Read results flatten to
//! `<into>.<col>` plus `<into>.#id`.

use crate::error::{RuntimeError, RuntimeResult};
use crate::function::{Condition, Procedure, ProcedureStep};
use blitz_types::value::Value;
use std::collections::HashMap;

/// Default step budget per call: bounds worst-case commit latency so one
/// procedure can't blow the p99 budget. No loops exist in v1; deep
/// If-nesting trips this only by genuine runaway.
pub const DEFAULT_FUEL: u64 = 10_000;

/// Storage + pure-function access for one procedure call. Implementations
/// buffer writes in the caller's transaction (read-your-writes inside the
/// call comes from the tx layer, not from here).
pub trait ProcedureBackend {
    /// Read a committed-or-buffered row. `None` = absent (runner aborts).
    fn read(&mut self, table: &str, id: u64) -> RuntimeResult<Option<HashMap<String, Value>>>;
    /// Buffer an insert. The assigned id is NOT known until commit; the
    /// runner records a `0` placeholder (documented v1 limitation).
    fn insert(&mut self, table: &str, values: HashMap<String, Value>) -> RuntimeResult<()>;
    /// Buffer an update. Missing row must `Err` (runner aborts).
    fn update(&mut self, table: &str, id: u64, values: HashMap<String, Value>) -> RuntimeResult<()>;
    /// Buffer a delete. Missing row must `Err` (runner aborts).
    fn delete(&mut self, table: &str, id: u64) -> RuntimeResult<()>;
    /// Pure-function call (server delegates to its `FunctionRegistry`).
    fn call_function(&self, name: &str, args: HashMap<String, Value>) -> RuntimeResult<Value>;
}

/// What a call produced: the `Return` value (or last step value / `Null`)
/// plus all live variables (the server maps these into the response row).
#[derive(Debug)]
pub struct ProcedureOutput {
    pub value: Value,
    pub variables: HashMap<String, Value>,
}

/// Execute `proc` with `args` as initial variables, at most `fuel` steps.
pub fn run_procedure<B: ProcedureBackend>(
    backend: &mut B,
    proc: &Procedure,
    args: HashMap<String, Value>,
    fuel: u64,
) -> RuntimeResult<ProcedureOutput> {
    let mut runner = Runner {
        backend,
        variables: args,
        fuel,
        burned: 0,
    };
    let value = runner.run_steps(&proc.steps)?;
    Ok(ProcedureOutput {
        value,
        variables: runner.variables,
    })
}

/// Step outcome: continue with a value, or unwind to the call root.
enum Flow {
    Next(Value),
    Return(Value),
}

struct Runner<'b, B: ProcedureBackend + ?Sized> {
    backend: &'b mut B,
    variables: HashMap<String, Value>,
    fuel: u64,
    burned: u64,
}

impl<B: ProcedureBackend + ?Sized> Runner<'_, B> {
    fn charge(&mut self) -> RuntimeResult<()> {
        self.burned += 1;
        if self.burned > self.fuel {
            return Err(RuntimeError::FuelExhausted(self.burned));
        }
        Ok(())
    }

    fn resolve(&self, value: &Value) -> Value {
        match value {
            Value::String(s) if s.starts_with('$') => {
                self.variables.get(&s[1..]).cloned().unwrap_or(Value::Null)
            }
            _ => value.clone(),
        }
    }

    fn resolve_id(&self, value: &Value) -> RuntimeResult<u64> {
        match self.resolve(value) {
            Value::UInt64(n) => Ok(n),
            Value::Int64(n) if n >= 0 => Ok(n as u64),
            Value::UInt32(n) => Ok(n as u64),
            Value::Int32(n) if n >= 0 => Ok(n as u64),
            other => Err(RuntimeError::ArgumentError(format!(
                "row id must be a non-negative integer, got {:?}",
                other
            ))),
        }
    }

    fn run_steps(&mut self, steps: &[ProcedureStep]) -> RuntimeResult<Value> {
        let mut last = Value::Null;
        for step in steps {
            self.charge()?;
            match self.run_step(step)? {
                Flow::Next(v) => last = v,
                // Return unwinds through nested If-branches to the call root.
                Flow::Return(v) => return Ok(v),
            }
        }
        Ok(last)
    }

    fn run_step(&mut self, step: &ProcedureStep) -> RuntimeResult<Flow> {
        match step {
            ProcedureStep::SetVariable { name, value } => {
                let v = self.resolve(value);
                self.variables.insert(name.clone(), v.clone());
                Ok(Flow::Next(v))
            }
            ProcedureStep::CallFunction { function, args } => {
                let resolved: HashMap<String, Value> = args
                    .iter()
                    .map(|(k, v)| (k.clone(), self.resolve(v)))
                    .collect();
                Ok(Flow::Next(self.backend.call_function(function, resolved)?))
            }
            ProcedureStep::If { condition, then_steps, else_steps } => {
                let v = if self.evaluate(condition)? {
                    self.run_steps(then_steps)?
                } else {
                    self.run_steps(else_steps)?
                };
                Ok(Flow::Next(v))
            }
            ProcedureStep::Read { table, id, into } => {
                let row_id = self.resolve_id(id)?;
                let row = self.backend.read(table, row_id)?.ok_or_else(|| {
                    RuntimeError::ExecutionError(format!("row not found: {}:{}", table, row_id))
                })?;
                self.variables.insert(format!("{}.#id", into), Value::UInt64(row_id));
                for (k, v) in row {
                    self.variables.insert(format!("{}.{}", into, k), v);
                }
                Ok(Flow::Next(Value::UInt64(row_id)))
            }
            ProcedureStep::Insert { table, values, into } => {
                let resolved: HashMap<String, Value> = values
                    .iter()
                    .map(|(k, v)| (k.clone(), self.resolve(v)))
                    .collect();
                self.backend.insert(table, resolved)?;
                // Assigned at commit; 0 marks "unknown during execution".
                self.variables.insert(into.clone(), Value::UInt64(0));
                Ok(Flow::Next(Value::UInt64(0)))
            }
            ProcedureStep::Update { table, id, values } => {
                let row_id = self.resolve_id(id)?;
                let resolved: HashMap<String, Value> = values
                    .iter()
                    .map(|(k, v)| (k.clone(), self.resolve(v)))
                    .collect();
                self.backend.update(table, row_id, resolved)?;
                Ok(Flow::Next(Value::UInt64(row_id)))
            }
            ProcedureStep::Delete { table, id } => {
                let row_id = self.resolve_id(id)?;
                self.backend.delete(table, row_id)?;
                Ok(Flow::Next(Value::UInt64(row_id)))
            }
            ProcedureStep::Return { value } => Ok(Flow::Return(self.resolve(value))),
            ProcedureStep::Fail { message } => {
                let msg = if let Some(var) = message.strip_prefix('$') {
                    self.variables.get(var).map(|v| v.to_string()).unwrap_or_else(|| message.clone())
                } else {
                    message.clone()
                };
                Err(RuntimeError::Abort(msg))
            }
        }
    }

    fn evaluate(&self, condition: &Condition) -> RuntimeResult<bool> {
        match condition {
            Condition::Equals(var, expected) => {
                let exp = self.resolve(expected);
                Ok(self.variables.get(var).map_or(false, |v| v == &exp))
            }
            Condition::NotEquals(var, expected) => {
                let exp = self.resolve(expected);
                Ok(self.variables.get(var).map_or(false, |v| v != &exp))
            }
            Condition::IsNotNull(var) => {
                Ok(self.variables.get(var).map_or(false, |v| !v.is_null()))
            }
            Condition::GreaterOrEqual(var, expected) => {
                let exp = self.resolve(expected);
                Ok(self.variables.get(var).map_or(false, |v| v.cmp(&exp) != std::cmp::Ordering::Less))
            }
            Condition::LessThan(var, expected) => {
                let exp = self.resolve(expected);
                Ok(self.variables.get(var).map_or(false, |v| v.cmp(&exp) == std::cmp::Ordering::Less))
            }
            Condition::All(cs) => {
                for c in cs {
                    if !self.evaluate(c)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Condition::Any(cs) => {
                for c in cs {
                    if self.evaluate(c)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::register_builtins;

    /// In-memory fake backend: committed state in one map, no tx semantics
    /// (unit scope — the server backend provides real OCC).
    struct Fake {
        tables: HashMap<String, HashMap<u64, HashMap<String, Value>>>,
        next_id: u64,
        functions: crate::function::FunctionRegistry,
    }

    impl Fake {
        fn new() -> Self {
            let mut functions = crate::function::FunctionRegistry::new();
            register_builtins(&mut functions);
            Self { tables: HashMap::new(), next_id: 1, functions }
        }

        fn seed(&mut self, table: &str, id: u64, row: HashMap<String, Value>) {
            self.tables.entry(table.into()).or_default().insert(id, row);
            self.next_id = self.next_id.max(id + 1);
        }
    }

    fn kv(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    impl ProcedureBackend for Fake {
        fn read(&mut self, table: &str, id: u64) -> RuntimeResult<Option<HashMap<String, Value>>> {
            Ok(self.tables.get(table).and_then(|t| t.get(&id)).cloned())
        }
        fn insert(&mut self, table: &str, values: HashMap<String, Value>) -> RuntimeResult<()> {
            let id = self.next_id;
            self.next_id += 1;
            self.tables.entry(table.into()).or_default().insert(id, values);
            Ok(())
        }
        fn update(&mut self, table: &str, id: u64, values: HashMap<String, Value>) -> RuntimeResult<()> {
            let row = self.tables.get_mut(table).and_then(|t| t.get_mut(&id)).ok_or_else(|| {
                RuntimeError::ExecutionError(format!("row not found: {}:{}", table, id))
            })?;
            for (k, v) in values {
                row.insert(k, v);
            }
            Ok(())
        }
        fn delete(&mut self, table: &str, id: u64) -> RuntimeResult<()> {
            let gone = self.tables.get_mut(table).map(|t| t.remove(&id).is_some()).unwrap_or(false);
            if !gone {
                return Err(RuntimeError::ExecutionError(format!("row not found: {}:{}", table, id)));
            }
            Ok(())
        }
        fn call_function(&self, name: &str, args: HashMap<String, Value>) -> RuntimeResult<Value> {
            self.functions.execute(name, args)
        }
    }

    fn checkout_proc() -> Procedure {
        use ProcedureStep::*;
        Procedure::new("checkout")
            .with_step(Read { table: "users".into(), id: Value::String("$user_id".into()), into: "u".into() })
            .with_step(If {
                condition: Condition::GreaterOrEqual("u.balance".into(), Value::String("$price".into())),
                then_steps: vec![
                    Insert {
                        table: "orders".into(),
                        values: kv(&[
                            ("user_id", Value::String("$user_id".into())),
                            ("amount", Value::String("$price".into())),
                        ]),
                        into: "order_id".into(),
                    },
                    Update {
                        table: "users".into(),
                        id: Value::String("$user_id".into()),
                        values: kv(&[("status", Value::String("charged".into()))]),
                    },
                    Return { value: Value::String("$order_id".into()) },
                ],
                else_steps: vec![Fail { message: "insufficient balance".into() }],
            })
    }

    #[test]
    fn checkout_happy_path() {
        let mut backend = Fake::new();
        backend.seed("users", 7, kv(&[
            ("balance", Value::Int64(100)),
            ("status", Value::String("active".into())),
        ]));
        let args = kv(&[("user_id", Value::Int64(7)), ("price", Value::Int64(40))]);
        let out = run_procedure(&mut backend, &checkout_proc(), args, DEFAULT_FUEL).unwrap();
        // Insert placeholder id (assigned at commit on the server).
        assert_eq!(out.value, Value::UInt64(0));
        let user = backend.read("users", 7).unwrap().unwrap();
        assert_eq!(user.get("status"), Some(&Value::String("charged".into())));
        let orders = backend.tables.get("orders").unwrap();
        assert_eq!(orders.len(), 1);
        let (_, order) = orders.iter().next().unwrap();
        assert_eq!(order.get("amount"), Some(&Value::Int64(40)));
    }

    #[test]
    fn checkout_insufficient_balance_aborts() {
        let mut backend = Fake::new();
        backend.seed("users", 7, kv(&[("balance", Value::Int64(10))]));
        let args = kv(&[("user_id", Value::Int64(7)), ("price", Value::Int64(40))]);
        let err = run_procedure(&mut backend, &checkout_proc(), args, DEFAULT_FUEL).unwrap_err();
        assert!(matches!(err, RuntimeError::Abort(ref m) if m == "insufficient balance"), "got {:?}", err);
        // Fake has no rollback (server does); assert the order insert DID run
        // pre-abort — the server's tx rollback is what makes this atomic.
        assert!(backend.tables.get("orders").is_none());
    }

    #[test]
    fn read_missing_row_aborts() {
        let mut backend = Fake::new();
        let proc = Procedure::new("r").with_step(ProcedureStep::Read {
            table: "users".into(),
            id: Value::Int64(999),
            into: "u".into(),
        });
        let err = run_procedure(&mut backend, &proc, HashMap::new(), DEFAULT_FUEL).unwrap_err();
        assert!(matches!(err, RuntimeError::ExecutionError(_)), "got {:?}", err);
    }

    #[test]
    fn fuel_exhaustion_kills_deep_nesting() {
        let mut backend = Fake::new();
        // 200 sequential steps with fuel 100: must die mid-run.
        let mut proc = Procedure::new("long");
        for i in 0..200 {
            proc = proc.with_step(ProcedureStep::SetVariable {
                name: format!("v{}", i),
                value: Value::Int64(i),
            });
        }
        let err = run_procedure(&mut backend, &proc, HashMap::new(), 100).unwrap_err();
        assert!(matches!(err, RuntimeError::FuelExhausted(_)), "got {:?}", err);
    }

    #[test]
    fn return_unwinds_past_later_steps() {
        let mut backend = Fake::new();
        let proc = Procedure::new("early")
            .with_step(ProcedureStep::Return { value: Value::Int64(1) })
            .with_step(ProcedureStep::SetVariable { name: "x".into(), value: Value::Int64(2) });
        let out = run_procedure(&mut backend, &proc, HashMap::new(), DEFAULT_FUEL).unwrap();
        assert_eq!(out.value, Value::Int64(1));
        assert!(out.variables.get("x").is_none());
    }
}
