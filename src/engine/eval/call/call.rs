use std::ops::Deref;

use super::super::ast;
use super::super::value;

use crate::engine::eval::Eval;
use crate::engine::vm::Vm;
use crate::engine::vm::VmSourceType;
use crate::source::Source;
use crate::{FilterxError, FilterxResult};

use crate::engine::eval::call::builtin as call;

impl<'a> Eval<'a> for ast::ExprCall {
    type Output = value::Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        let mut function_name: String = match self.func.deref() {
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

        let mut inplace = false;
        let mut sub_function_name = "".to_string();
        if function_name.starts_with("cast_") {
            if function_name.ends_with("_") {
                inplace = true;
                function_name = function_name.strip_suffix("_").unwrap().to_string();
            }
            sub_function_name = function_name.strip_prefix("cast_").unwrap().to_string();
        }
        if function_name.starts_with("cast") {
            function_name = "cast".to_string();
        }

        match vm.source {
            Source::Dataframe(_) => match function_name.as_str() {
                "alias" => call::alias(vm, &self.args),
                "drop" => call::drop(vm, &self.args),
                "select" => call::select(vm, &self.args),
                "col" => call::col(vm, &self.args),
                "rename" => {
                    if vm.source_type == VmSourceType::Fasta
                        || vm.source_type == VmSourceType::Fastq
                    {
                        Err(FilterxError::RuntimeError(format!(
                            "Function `{}` does not be supported in source `{:?}`.",
                            function_name, vm.source_type
                        )))
                    } else {
                        call::rename(vm, &self.args)
                    }
                }
                "head" => call::head(vm, &self.args),
                "tail" => call::tail(vm, &self.args),
                "Sort" => call::sort(vm, &self.args, false),
                "sorT" => call::sort(vm, &self.args, true),
                "sort" => call::sort(vm, &self.args, true),
                "len" => call::len(vm, &self.args),
                "print" => call::print(vm, &self.args),
                "limit" => call::limit(vm, &self.args),
                "gc" => call::gc(vm, &self.args),
                "rev" => call::rev(vm, &self.args, false),
                "rev_" => call::rev(vm, &self.args, true),
                "revcomp" => call::revcomp(vm, &self.args, false),
                "revcomp_" => call::revcomp(vm, &self.args, true),
                "upper" => call::upper(vm, &self.args, false),
                "upper_" => call::upper(vm, &self.args, true),
                "lower" => call::lower(vm, &self.args, false),
                "lower_" => call::lower(vm, &self.args, true),
                "replace" => call::replace(vm, &self.args, false, true),
                "replace_" => call::replace(vm, &self.args, true, true),
                "replace_one" => call::replace(vm, &self.args, false, false),
                "replace_one_" => call::replace(vm, &self.args, true, false),
                "strip" => call::strip(vm, &self.args, false, true, true),
                "strip_" => call::strip(vm, &self.args, true, true, true),
                "lstrip" => call::strip(vm, &self.args, false, false, true),
                "lstrip_" => call::strip(vm, &self.args, true, false, true),
                "rstrip" => call::strip(vm, &self.args, false, true, false),
                "rstrip_" => call::strip(vm, &self.args, true, true, false),
                "slice" => call::slice(vm, &self.args, false),
                "slice_" => call::slice(vm, &self.args, true),
                "header" => call::header(vm),
                "cast" => call::cast(vm, &self.args, &sub_function_name, inplace),
                "fill" => call::fill(vm, &self.args, false),
                "fill_" => call::fill(vm, &self.args, true),
                _ => Err(FilterxError::RuntimeError(format!(
                    "Function `{}` is not defined.",
                    function_name
                ))),
            },
        }
    }
}

mod test {}
