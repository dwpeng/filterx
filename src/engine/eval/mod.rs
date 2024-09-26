pub mod assign;
pub mod call;
pub mod name;
pub mod ops;

use std::ops::Deref;

use crate::{FilterxError, FilterxResult};

use super::ast;
use super::value;
use super::vm::Vm;

pub trait Eval<'a> {
    type Output;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output>;
}

impl<'a> Eval<'a> for ast::ModExpression {
    type Output = value::Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        match self.body.deref() {
            // in/not in
            // >, <, >=, <=, ==, !=
            ast::Expr::Compare(c) => {
                let v = c.eval(vm)?;
                Ok(v)
            }
            // and, or
            ast::Expr::BoolOp(b) => {
                let v = b.eval(vm)?;
                Ok(v)
            }
            // a.b
            // a(1)
            ast::Expr::Call(c) => {
                let v = c.eval(vm)?;
                Ok(v)
            }
            ast::Expr::Tuple(t) => {
                let v = t.eval(vm)?;
                Ok(v)
            }
            _ => {
                let err = format!("A expr is required. But got {}", vm.eval_expr);
                return Err(FilterxError::RuntimeError(err));
            }
        }
    }
}

impl<'a> Eval<'a> for ast::ModInteractive {
    type Output = value::Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        if self.body.len() != 1 {
            return Err(FilterxError::RuntimeError(
                "Only support one line".to_string(),
            ));
        }
        let stmt = self.body.first().unwrap();
        match stmt {
            ast::Stmt::Assign(a) => {
                let v = a.eval(vm)?;
                Ok(v)
            }
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support assign operate, like \"alias(new_col) = col1 + col2\""
                        .to_string(),
                ));
            }
        }
    }
}
