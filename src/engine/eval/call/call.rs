use std::ops::Deref;

use super::super::ast;
use super::super::value;

use crate::engine::eval::Eval;
use crate::engine::vm::Vm;
use crate::source::Source;
use crate::{FilterxError, FilterxResult};

use crate::engine::eval::call;

use super::BUILTIN_FUNCTIONS;

impl<'a> Eval<'a> for ast::ExprCall {
    type Output = value::Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        let function_name = match self.func.deref() {
            ast::Expr::Name(e) => {
                let v = e.eval(vm)?;
                v.text()?
            }
            ast::Expr::Attribute(a) => {
                let v = a.eval(vm)?;
                return Ok(v);
            }
            ast::Expr::Call(c) => {
                let v = c.eval(vm)?;
                return Ok(v);
            }
            _ => unreachable!(),
        };
        // a buildin function
        if !BUILTIN_FUNCTIONS.contains(&function_name.as_str()) {
            return Err(FilterxError::RuntimeError(format!(
                "Function `{}` is not defined.",
                function_name
            )));
        }

        match vm.source {
            Source::Dataframe(_) => match function_name.as_str() {
                "Alias" => call::Alias(vm, &self.args),
                "alias" => call::alias(vm, &self.args),
                "drop" => call::drop(vm, &self.args),
                "select" => call::select(vm, &self.args),
                "col" => call::col(vm, &self.args),
                _ => Err(FilterxError::RuntimeError(format!(
                    "Function `{}` is not defined.",
                    function_name
                ))),
            },
            Source::Fasta(_) => match function_name.as_str() {
                "Alias" => call::Alias(vm, &self.args),
                "alias" => call::alias(vm, &self.args),
                "drop" => call::drop(vm, &self.args),
                "select" => call::select(vm, &self.args),
                "col" => call::col(vm, &self.args),
                _ => Err(FilterxError::RuntimeError(format!(
                    "Function `{}` is not defined.",
                    function_name
                ))),
            },
            Source::Fastq(_) => match function_name.as_str() {
                "Alias" => call::Alias(vm, &self.args),
                "alias" => call::alias(vm, &self.args),
                "drop" => call::drop(vm, &self.args),
                "select" => call::select(vm, &self.args),
                "col" => call::col(vm, &self.args),
                _ => Err(FilterxError::RuntimeError(format!(
                    "Function `{}` is not defined.",
                    function_name
                ))),
            },
        }
    }
}

mod test {}
