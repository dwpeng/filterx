use std::ops::Deref;

use polars::prelude::*;

use super::super::ast;
use super::super::value::Value;

use crate::engine::eval::Eval;
use crate::engine::vm::Vm;
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

        let value = match right {
            ast::Expr::BinOp(o) => o.eval(vm)?,
            ast::Expr::Constant(c) => c.eval(vm)?,
            ast::Expr::Name(n) => n.eval(vm)?,
            ast::Expr::Call(c) => c.eval(vm)?,
            ast::Expr::UnaryOp(u) => u.eval(vm)?,
            _ => {
                return Err(FilterxError::RuntimeError(
                    "use `alias` to create a new column, like alias(new_col) = col1 + col2"
                        .to_string(),
                ))
            }
        };

        // TODO
        // pass percise type
        vm.status
            .add_new_column(&new_col, DataType::Unknown(UnknownKind::Any));

        match vm.source {
            Source::Dataframe(ref mut df_source) => {
                assign_in_dataframe(value, new_col, df_source)?;
            }
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support dataframe.".to_string(),
                ));
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
    match value {
        Value::Column(c) => {
            let lazy = df_source.lazy.clone();
            let lazy = lazy.with_column(col(c.col_name).alias(new_col));
            df_source.lazy = lazy;
        }

        Value::Int(i) => {
            let lazy = df_source.lazy.clone();
            let lazy = lazy.with_column(lit(i).alias(new_col));
            df_source.lazy = lazy;
        }

        Value::Float(f) => {
            let lazy = df_source.lazy.clone();
            let lazy = lazy.with_column(lit(f).alias(new_col));
            df_source.lazy = lazy;
        }

        Value::Str(s) => {
            let lazy = df_source.lazy.clone();
            let lazy = lazy.with_column(lit(s).alias(new_col));
            df_source.lazy = lazy;
        }

        Value::MultiColumn(m) => {
            let lazy = df_source.lazy.clone();
            let mut expr = m.left.expr()?;
            for (i, op) in m.op.iter().enumerate() {
                let other = m.other[i].clone();
                expr = match op {
                    ast::CmpOp::Eq => expr.eq(other.expr()?),
                    ast::CmpOp::Gt => expr.gt(other.expr()?),
                    ast::CmpOp::NotEq => expr.neq(other.expr()?),
                    ast::CmpOp::Lt => expr.lt(other.expr()?),
                    ast::CmpOp::LtE => expr.lt_eq(other.expr()?),
                    ast::CmpOp::GtE => expr.gt_eq(other.expr()?),
                    _ => {
                        return Err(FilterxError::RuntimeError(
                            format!("Not support {:?}.", op).into(),
                        ))
                    }
                };
            }
            let lazy = lazy.with_column(expr);
            df_source.lazy = lazy;
        }
        Value::Expr(e) => {
            let lazy = df_source.lazy.clone();
            let lazy = lazy.with_column(e.alias(new_col));
            df_source.lazy = lazy;
        }
        _ => {
            return Err(FilterxError::RuntimeError(
                "use `alias` to create a new column, like alias(new_col) = col1 + col2".to_string(),
            ))
        }
    }

    Ok(Value::None)
}
