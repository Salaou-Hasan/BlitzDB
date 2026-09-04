use crate::function::{Condition, FunctionRegistry, Procedure, ProcedureStep};
use crate::error::RuntimeResult;
use blitz_types::value::Value;
use std::collections::HashMap;

/// The application runtime executor.
pub struct RuntimeExecutor {
    pub functions: FunctionRegistry,
    procedures: HashMap<String, Procedure>,
    variables: HashMap<String, Value>,
}

impl RuntimeExecutor {
    pub fn new() -> Self {
        Self {
            functions: FunctionRegistry::new(),
            procedures: HashMap::new(),
            variables: HashMap::new(),
        }
    }

    /// Create a new executor with all built-in functions registered.
    pub fn with_builtins() -> Self {
        let mut executor = Self::new();
        crate::function::register_builtins(&mut executor.functions);
        executor
    }

    /// Register a procedure.
    pub fn register_procedure(&mut self, procedure: Procedure) {
        self.procedures.insert(procedure.name.clone(), procedure);
    }

    /// Get a procedure by name.
    pub fn get_procedure(&self, name: &str) -> Option<&Procedure> {
        self.procedures.get(name)
    }

    /// Set a variable in the runtime context.
    pub fn set_variable(&mut self, name: impl Into<String>, value: Value) {
        self.variables.insert(name.into(), value);
    }

    /// Get a variable from the runtime context.
    pub fn get_variable(&self, name: &str) -> Option<&Value> {
        self.variables.get(name)
    }

    /// Execute a function by name.
    pub fn execute_function(
        &self,
        name: &str,
        args: HashMap<String, Value>,
    ) -> RuntimeResult<Value> {
        self.functions.execute(name, args)
    }

    /// Execute a stored procedure by name.
    pub fn execute_procedure(&mut self, name: &str) -> RuntimeResult<Value> {
        let procedure = self
            .procedures
            .get(name)
            .cloned()
            .ok_or_else(|| crate::RuntimeError::FunctionNotFound(name.to_string()))?;

        let mut last_value = Value::Null;
        for step in &procedure.steps {
            last_value = self.execute_step(step)?;
        }
        Ok(last_value)
    }

    fn execute_step(&mut self, step: &ProcedureStep) -> RuntimeResult<Value> {
        match step {
            ProcedureStep::SetVariable { name, value } => {
                self.variables.insert(name.clone(), value.clone());
                Ok(value.clone())
            }
            ProcedureStep::CallFunction { function, args } => {
                let resolved_args: HashMap<String, Value> = args
                    .iter()
                    .map(|(k, v)| (k.clone(), self.resolve_value(v)))
                    .collect();
                self.execute_function(function, resolved_args)
            }
            ProcedureStep::If {
                condition,
                then_steps,
                else_steps,
            } => {
                let result = self.evaluate_condition(condition)?;
                let steps = if result { then_steps } else { else_steps };
                let mut last_value = Value::Null;
                for step in steps {
                    last_value = self.execute_step(step)?;
                }
                Ok(last_value)
            }
        }
    }

    fn resolve_value(&self, value: &Value) -> Value {
        match value {
            Value::String(s) if s.starts_with('$') => {
                let var_name = &s[1..];
                self.variables
                    .get(var_name)
                    .cloned()
                    .unwrap_or(Value::Null)
            }
            _ => value.clone(),
        }
    }

    fn evaluate_condition(&self, condition: &Condition) -> RuntimeResult<bool> {
        match condition {
            Condition::Equals(var_name, expected) => {
                let actual = self.variables.get(var_name);
                Ok(actual.map_or(false, |v| v == expected))
            }
            Condition::IsNotNull(var_name) => {
                Ok(self.variables.get(var_name).map_or(false, |v| !v.is_null()))
            }
            Condition::All(conditions) => {
                for c in conditions {
                    if !self.evaluate_condition(c)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Condition::Any(conditions) => {
                for c in conditions {
                    if self.evaluate_condition(c)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }
}

impl Default for RuntimeExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_builtins() {
        let executor = RuntimeExecutor::with_builtins();
        assert!(executor.functions.has("now"));
        assert!(executor.functions.has("concat"));
    }

    #[test]
    fn test_execute_function() {
        let executor = RuntimeExecutor::with_builtins();
        let mut args: HashMap<String, Value> = HashMap::new();
        args.insert("0".into(), Value::String("hello".into()));
        let result = executor.execute_function("upper", args).unwrap();
        assert_eq!(result, Value::String("HELLO".into()));
    }

    #[test]
    fn test_variables() {
        let mut executor = RuntimeExecutor::new();
        executor.set_variable("x", Value::Int64(42));
        assert_eq!(
            executor.get_variable("x"),
            Some(&Value::Int64(42))
        );
    }

    #[test]
    fn test_procedure_set_variable() {
        let mut executor = RuntimeExecutor::new();
        let proc = Procedure::new("test")
            .with_step(ProcedureStep::SetVariable {
                name: "greeting".into(),
                value: Value::String("hello".into()),
            });
        executor.register_procedure(proc);
        executor.execute_procedure("test").unwrap();
        assert_eq!(
            executor.get_variable("greeting"),
            Some(&Value::String("hello".into()))
        );
    }

    #[test]
    fn test_procedure_call_function() {
        let mut executor = RuntimeExecutor::with_builtins();

        let proc = Procedure::new("test")
            .with_step(ProcedureStep::SetVariable {
                name: "input".into(),
                value: Value::String("hello".into()),
            })
            .with_step(ProcedureStep::CallFunction {
                function: "upper".into(),
                args: {
                    let mut m = HashMap::new();
                    m.insert("0".into(), Value::String("hello".into()));
                    m
                },
            });

        executor.register_procedure(proc);
        let result = executor.execute_procedure("test").unwrap();
        assert_eq!(result, Value::String("HELLO".into()));
    }

    #[test]
    fn test_procedure_if_condition() {
        let mut executor = RuntimeExecutor::new();

        let proc = Procedure::new("test")
            .with_step(ProcedureStep::SetVariable {
                name: "role".into(),
                value: Value::String("admin".into()),
            })
            .with_step(ProcedureStep::If {
                condition: Condition::Equals("role".into(), Value::String("admin".into())),
                then_steps: vec![ProcedureStep::SetVariable {
                    name: "access".into(),
                    value: Value::String("granted".into()),
                }],
                else_steps: vec![ProcedureStep::SetVariable {
                    name: "access".into(),
                    value: Value::String("denied".into()),
                }],
            });

        executor.register_procedure(proc);
        executor.execute_procedure("test").unwrap();
        assert_eq!(
            executor.get_variable("access"),
            Some(&Value::String("granted".into()))
        );
    }

    #[test]
    fn test_procedure_if_else() {
        let mut executor = RuntimeExecutor::new();

        let proc = Procedure::new("test")
            .with_step(ProcedureStep::SetVariable {
                name: "role".into(),
                value: Value::String("viewer".into()),
            })
            .with_step(ProcedureStep::If {
                condition: Condition::Equals("role".into(), Value::String("admin".into())),
                then_steps: vec![ProcedureStep::SetVariable {
                    name: "access".into(),
                    value: Value::String("granted".into()),
                }],
                else_steps: vec![ProcedureStep::SetVariable {
                    name: "access".into(),
                    value: Value::String("denied".into()),
                }],
            });

        executor.register_procedure(proc);
        executor.execute_procedure("test").unwrap();
        assert_eq!(
            executor.get_variable("access"),
            Some(&Value::String("denied".into()))
        );
    }

    #[test]
    fn test_procedure_not_found() {
        let mut executor = RuntimeExecutor::new();
        let result = executor.execute_procedure("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_condition_is_not_null() {
        let mut executor = RuntimeExecutor::new();
        executor.set_variable("x", Value::Int64(1));
        assert!(executor.evaluate_condition(&Condition::IsNotNull("x".into())).unwrap());
        assert!(!executor.evaluate_condition(&Condition::IsNotNull("missing".into())).unwrap());
    }

    #[test]
    fn test_condition_all_any() {
        let mut executor = RuntimeExecutor::new();
        executor.set_variable("a", Value::Int64(1));
        executor.set_variable("b", Value::Int64(2));

        let all = Condition::All(vec![
            Condition::IsNotNull("a".into()),
            Condition::IsNotNull("b".into()),
        ]);
        assert!(executor.evaluate_condition(&all).unwrap());

        let any = Condition::Any(vec![
            Condition::Equals("a".into(), Value::Int64(99)),
            Condition::IsNotNull("b".into()),
        ]);
        assert!(executor.evaluate_condition(&any).unwrap());
    }
}
