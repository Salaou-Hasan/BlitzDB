use blitz_types::value::Value;
use std::collections::HashMap;

/// Trait for application-defined functions.
pub trait AppFunction: Send + Sync {
    fn name(&self) -> &str;
    fn execute(&self, args: HashMap<String, Value>) -> super::RuntimeResult<Value>;
}

/// A concrete function created from a closure.
pub struct FnFunction {
    name: String,
    func: Box<dyn Fn(HashMap<String, Value>) -> super::RuntimeResult<Value> + Send + Sync>,
}

impl FnFunction {
    pub fn new(
        name: impl Into<String>,
        func: impl Fn(HashMap<String, Value>) -> super::RuntimeResult<Value> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            func: Box::new(func),
        }
    }
}

impl AppFunction for FnFunction {
    fn name(&self) -> &str {
        &self.name
    }

    fn execute(&self, args: HashMap<String, Value>) -> super::RuntimeResult<Value> {
        (self.func)(args)
    }
}

/// A stored procedure is a named sequence of steps.
/// Serializable (deploy envelope carries this shape as JSON; see
/// `validate_procedure` for deploy-time rules).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Procedure {
    pub name: String,
    pub description: String,
    pub steps: Vec<ProcedureStep>,
}

/// A single step in a procedure.
///
/// `$var` references in any `Value::String` resolve against live variables
/// (same convention as `CallFunction` args). DB steps run inside the
/// caller's transaction via [`ProcedureBackend`](crate::ProcedureBackend).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ProcedureStep {
    /// Call a function by name with arguments.
    CallFunction {
        function: String,
        args: HashMap<String, Value>,
    },
    /// Set a variable.
    SetVariable {
        name: String,
        value: Value,
    },
    /// Conditionally execute steps.
    If {
        condition: Condition,
        then_steps: Vec<ProcedureStep>,
        else_steps: Vec<ProcedureStep>,
    },
    /// Read a row: stores each column as `<into>.<col>` plus `<into>.#id`
    /// (the row id as `UInt64`). Missing row aborts the call.
    Read {
        table: String,
        id: Value,
        into: String,
    },
    /// Buffer an insert. `into` receives the assigned id as `UInt64` —
    /// placeholder `0` DURING execution (ids are assigned at commit);
    /// real ids are reported in the call response. Referencing a
    /// just-inserted row by id in later steps is unsupported (v1).
    Insert {
        table: String,
        values: HashMap<String, Value>,
        into: String,
    },
    /// Buffer an update (read-before-write: missing row aborts).
    Update {
        table: String,
        id: Value,
        values: HashMap<String, Value>,
    },
    /// Buffer a delete (missing row aborts).
    Delete {
        table: String,
        id: Value,
    },
    /// Return a value to the caller (ends execution). Non-`Json` values
    /// arrive as `{"result": v}`; `Json` objects flatten scalar tops.
    Return {
        value: Value,
    },
    /// Abort the call with a message: server rolls everything back.
    Fail {
        message: String,
    },
}

/// A condition that can be evaluated.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Condition {
    /// Variable equals a value.
    Equals(String, Value),
    /// Variable does not equal a value.
    NotEquals(String, Value),
    /// Variable is not null (and present).
    IsNotNull(String),
    /// Numeric/ordered `>=` via `Value`'s total order (Int64 vs Int64 in
    /// practice; mixed types order by variant — keep operands same-typed).
    GreaterOrEqual(String, Value),
    /// Ordered `<` via `Value`'s total order (same-type operands).
    LessThan(String, Value),
    /// All sub-conditions are true.
    All(Vec<Condition>),
    /// Any sub-condition is true.
    Any(Vec<Condition>),
}

impl Procedure {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            steps: Vec::new(),
        }
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    pub fn with_step(mut self, step: ProcedureStep) -> Self {
        self.steps.push(step);
        self
    }
}

/// Registry of named functions.
pub struct FunctionRegistry {
    functions: HashMap<String, Box<dyn AppFunction>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }

    /// Register a function.
    pub fn register(&mut self, func: Box<dyn AppFunction>) {
        self.functions
            .insert(func.name().to_string(), func);
    }

    /// Execute a registered function by name.
    pub fn execute(
        &self,
        name: &str,
        args: HashMap<String, Value>,
    ) -> super::RuntimeResult<Value> {
        let func = self
            .functions
            .get(name)
            .ok_or_else(|| super::RuntimeError::FunctionNotFound(name.to_string()))?;
        func.execute(args)
    }

    /// Check if a function is registered.
    pub fn has(&self, name: &str) -> bool {
        self.functions.contains_key(name)
    }

    /// List all registered function names.
    pub fn list(&self) -> Vec<&str> {
        self.functions.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Built-in functions ──

pub struct NowFn;
impl AppFunction for NowFn {
    fn name(&self) -> &str { "now" }
    fn execute(&self, _args: HashMap<String, Value>) -> super::RuntimeResult<Value> {
        Ok(Value::Timestamp(chrono::Utc::now()))
    }
}

pub struct ConcatFn;
impl AppFunction for ConcatFn {
    fn name(&self) -> &str { "concat" }
    fn execute(&self, args: HashMap<String, Value>) -> super::RuntimeResult<Value> {
        let mut result = String::new();
        let mut i = 0;
        loop {
            let key = format!("{}", i);
            match args.get(&key) {
                Some(Value::String(s)) => result.push_str(s),
                Some(v) => result.push_str(&v.to_string()),
                None => break,
            }
            i += 1;
        }
        Ok(Value::String(result))
    }
}

pub struct UpperFn;
impl AppFunction for UpperFn {
    fn name(&self) -> &str { "upper" }
    fn execute(&self, args: HashMap<String, Value>) -> super::RuntimeResult<Value> {
        let val = args
            .get("0")
            .or_else(|| args.get("value"))
            .ok_or_else(|| super::RuntimeError::ArgumentError("expected 'value' argument".into()))?;
        match val {
            Value::String(s) => Ok(Value::String(s.to_uppercase())),
            _ => Err(super::RuntimeError::ArgumentError("expected string".into())),
        }
    }
}

pub struct LowerFn;
impl AppFunction for LowerFn {
    fn name(&self) -> &str { "lower" }
    fn execute(&self, args: HashMap<String, Value>) -> super::RuntimeResult<Value> {
        let val = args
            .get("0")
            .or_else(|| args.get("value"))
            .ok_or_else(|| super::RuntimeError::ArgumentError("expected 'value' argument".into()))?;
        match val {
            Value::String(s) => Ok(Value::String(s.to_lowercase())),
            _ => Err(super::RuntimeError::ArgumentError("expected string".into())),
        }
    }
}

pub struct LenFn;
impl AppFunction for LenFn {
    fn name(&self) -> &str { "len" }
    fn execute(&self, args: HashMap<String, Value>) -> super::RuntimeResult<Value> {
        let val = args
            .get("0")
            .or_else(|| args.get("value"))
            .ok_or_else(|| super::RuntimeError::ArgumentError("expected 'value' argument".into()))?;
        match val {
            Value::String(s) => Ok(Value::Int64(s.len() as i64)),
            Value::Array(a) => Ok(Value::Int64(a.len() as i64)),
            _ => Err(super::RuntimeError::ArgumentError("expected string or array".into())),
        }
    }
}

pub struct AbsFn;
impl AppFunction for AbsFn {
    fn name(&self) -> &str { "abs" }
    fn execute(&self, args: HashMap<String, Value>) -> super::RuntimeResult<Value> {
        let val = args
            .get("0")
            .or_else(|| args.get("value"))
            .ok_or_else(|| super::RuntimeError::ArgumentError("expected 'value' argument".into()))?;
        match val {
            Value::Int64(n) => Ok(Value::Int64(n.abs())),
            Value::Float64(n) => Ok(Value::Float64(n.abs())),
            Value::Int32(n) => Ok(Value::Int32(n.abs())),
            Value::Float32(n) => Ok(Value::Float32(n.abs())),
            _ => Err(super::RuntimeError::ArgumentError("expected numeric".into())),
        }
    }
}

pub struct CoalesceFn;
impl AppFunction for CoalesceFn {
    fn name(&self) -> &str { "coalesce" }
    fn execute(&self, args: HashMap<String, Value>) -> super::RuntimeResult<Value> {
        let mut i = 0;
        loop {
            let key = format!("{}", i);
            match args.get(&key) {
                Some(v) if !v.is_null() => return Ok(v.clone()),
                Some(_) => { i += 1; }
                None => break,
            }
        }
        Ok(Value::Null)
    }
}

/// Register all built-in functions into a registry.
pub fn register_builtins(registry: &mut FunctionRegistry) {
    registry.register(Box::new(NowFn));
    registry.register(Box::new(ConcatFn));
    registry.register(Box::new(UpperFn));
    registry.register(Box::new(LowerFn));
    registry.register(Box::new(LenFn));
    registry.register(Box::new(AbsFn));
    registry.register(Box::new(CoalesceFn));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fn_registry() {
        let mut reg = FunctionRegistry::new();
        reg.register(Box::new(NowFn));
        assert!(reg.has("now"));
        assert!(!reg.has("missing"));
        let result = reg.execute("now", HashMap::new()).unwrap();
        assert!(matches!(result, Value::Timestamp(_)));
    }

    #[test]
    fn test_builtins_registered() {
        let mut reg = FunctionRegistry::new();
        register_builtins(&mut reg);
        let names = reg.list();
        assert!(names.contains(&"now"));
        assert!(names.contains(&"concat"));
        assert!(names.contains(&"upper"));
        assert!(names.contains(&"lower"));
        assert!(names.contains(&"len"));
        assert!(names.contains(&"abs"));
        assert!(names.contains(&"coalesce"));
    }

    #[test]
    fn test_concat() {
        let mut reg = FunctionRegistry::new();
        register_builtins(&mut reg);
        let mut args = HashMap::new();
        args.insert("0".into(), Value::String("hello".into()));
        args.insert("1".into(), Value::String(" ".into()));
        args.insert("2".into(), Value::String("world".into()));
        let result = reg.execute("concat", args).unwrap();
        assert_eq!(result, Value::String("hello world".into()));
    }

    #[test]
    fn test_upper() {
        let mut reg = FunctionRegistry::new();
        register_builtins(&mut reg);
        let mut args = HashMap::new();
        args.insert("0".into(), Value::String("hello".into()));
        let result = reg.execute("upper", args).unwrap();
        assert_eq!(result, Value::String("HELLO".into()));
    }

    #[test]
    fn test_lower() {
        let mut reg = FunctionRegistry::new();
        register_builtins(&mut reg);
        let mut args = HashMap::new();
        args.insert("0".into(), Value::String("HELLO".into()));
        let result = reg.execute("lower", args).unwrap();
        assert_eq!(result, Value::String("hello".into()));
    }

    #[test]
    fn test_len() {
        let mut reg = FunctionRegistry::new();
        register_builtins(&mut reg);
        let mut args = HashMap::new();
        args.insert("0".into(), Value::String("hello".into()));
        let result = reg.execute("len", args).unwrap();
        assert_eq!(result, Value::Int64(5));
    }

    #[test]
    fn test_abs() {
        let mut reg = FunctionRegistry::new();
        register_builtins(&mut reg);
        let mut args = HashMap::new();
        args.insert("0".into(), Value::Int64(-42));
        let result = reg.execute("abs", args).unwrap();
        assert_eq!(result, Value::Int64(42));
    }

    #[test]
    fn test_coalesce() {
        let mut reg = FunctionRegistry::new();
        register_builtins(&mut reg);
        let mut args = HashMap::new();
        args.insert("0".into(), Value::Null);
        args.insert("1".into(), Value::Null);
        args.insert("2".into(), Value::String("found".into()));
        let result = reg.execute("coalesce", args).unwrap();
        assert_eq!(result, Value::String("found".into()));
    }

    #[test]
    fn test_coalesce_all_null() {
        let mut reg = FunctionRegistry::new();
        register_builtins(&mut reg);
        let mut args = HashMap::new();
        args.insert("0".into(), Value::Null);
        let result = reg.execute("coalesce", args).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_procedure_builder() {
        let proc = Procedure::new("create_user")
            .with_description("Creates a new user")
            .with_step(ProcedureStep::SetVariable {
                name: "name".into(),
                value: Value::String("Alice".into()),
            });
        assert_eq!(proc.name, "create_user");
        assert_eq!(proc.description, "Creates a new user");
        assert_eq!(proc.steps.len(), 1);
    }
}
