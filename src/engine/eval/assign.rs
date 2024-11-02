use std::ops::Deref;

use super::super::ast;
use super::super::value::Value;

use crate::engine::eval::Eval;
use crate::engine::vm::Vm;
use crate::eval;
use crate::source::DataframeSource;
use crate::source::Source;
use crate::{FilterxError, FilterxResult};

impl<'a> Eval<'a> for ast::StmtAssign {
    type Output = Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        if self.targets.len() != 1 {
            return Err(FilterxError::RuntimeError(
                "Only support one target".to_string(),
            ));
        }
        let target = self.targets.first().unwrap();
        let new_col = match target {
            ast::Expr::Call(c) => c.eval(vm)?,
            _ => {
                return Err(FilterxError::RuntimeError(
                    "use `alias`/`Alias` to create a new column, like alias(new_col) = col1 + col2"
                        .to_string(),
                ));
            }
        };

        let new_col = match new_col {
            Value::Column(c) => {
                if c.new == false && c.force == false {
                    return Err(FilterxError::RuntimeError(format!(
                        "Column `{}` already exist, use `Alias({})` to force creation.",
                        c.col_name, c.col_name
                    )));
                }
                c.col_name
            }

            _ => {
                return Err(FilterxError::RuntimeError(
                    "use `alias` to create a new column, like alias(new_col) = col1 + col2"
                        .to_string(),
                ));
            }
        };

        let right = self.value.deref();

        let value = eval!(
            vm,
            right,
            "use `alias` to create a new column, like alias(new_col) = col1 + col2",
            Constant,
            Name,
            Call,
            UnaryOp,
            BinOp
        );

        match vm.source {
            Source::Dataframe(ref mut df_source) => {
                assign_in_dataframe(value, new_col, df_source)?;
            }
        }

        Ok(Value::None)
    }
}

fn assign_in_dataframe<'a>(
    value: Value,
    new_col: String,
    df_source: &'a mut DataframeSource,
) -> FilterxResult<Value> {
    let lazy = df_source.lazy.clone();
    let lazy = lazy.with_column(value.expr()?.alias(new_col));
    df_source.update(lazy);
    Ok(Value::None)
}
