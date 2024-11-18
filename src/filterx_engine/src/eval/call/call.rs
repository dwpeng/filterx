use std::ops::Deref;

use polars::frame::UniqueKeepStrategy;

use super::super::ast;

use crate::vm::{Vm, VmMode};
use filterx_core::{value, FilterxResult};
use filterx_source::source::SourceType;

use super::functions::get_function;
use crate::eval::call::builtin as call;
use crate::eval::Eval;

fn compute_similarity(_target: &str, _reference: Vec<&'static str>) -> Option<&'static str> {
    None
}

impl<'a> Eval<'a> for ast::ExprCall {
    type Output = value::Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        let original_function_name: String = match self.func.deref() {
            ast::Expr::Name(e) => {
                let v = e.eval(vm)?;
                v.text()?
            }
            ast::Expr::Attribute(a) => {
                let v = a.eval(vm)?;
                return Ok(v);
            }
            _ => unreachable!(),
        };

        let mut function_name = original_function_name.as_str();
        let inplace = function_name.ends_with("_");
        let mut original_function_name = function_name;

        if inplace {
            function_name = function_name.strip_suffix("_").unwrap();
            original_function_name = function_name;
        }
        let mut sub_function_name = "";

        if function_name.starts_with("cast_") {
            sub_function_name = function_name.strip_prefix("cast_").unwrap();
        }
        if function_name.starts_with("cast") {
            function_name = "cast";
        }

        // TODO
        let f = get_function(function_name);

        if vm.mode == VmMode::Printable {
            if let Some(f) = f {
                if !f.can_expression {
                    let h = &mut vm.hint;
                    h.white("Function `")
                        .cyan(&original_function_name)
                        .bold()
                        .white("` can not be used in ")
                        .green("`print`")
                        .bold()
                        .white(" formatter. But got ")
                        .cyan(&vm.print_expr)
                        .white(".")
                        .print_and_exit();
                }

                if inplace {
                    let h = &mut vm.hint;
                    h.white("Function `")
                        .cyan(&original_function_name)
                        .bold()
                        .white("(")
                        .cyan("inplace")
                        .white(")` can not be used in ")
                        .green("`print`")
                        .bold()
                        .white(" formatter.")
                        .print_and_exit();
                }
            }
        }

        match function_name {
            "alias" => call::alias(vm, &self.args),
            "drop" => call::drop(vm, &self.args),
            "select" => call::select(vm, &self.args),
            "col" | "c" => call::col(vm, &self.args),
            "rename" => {
                if vm.source_type() == SourceType::Fasta || vm.source_type() == SourceType::Fastq {
                    let source_type = vm.source_type();
                    let h = &mut vm.hint;
                    h.white("Function `rename` does not be supported in source `")
                        .cyan(&format!("{:?}", source_type))
                        .white("`.")
                        .print_and_exit();
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
            "print" | "format" | "fmt" | "f" => call::print(vm, &self.args),
            "limit" => call::limit(vm, &self.args),
            "gc" => call::gc(vm, &self.args),
            "qual" => call::qual(vm, &self.args),
            "phred" => call::phred(vm),
            "rev" => call::rev(vm, &self.args, inplace),
            "revcomp" => call::revcomp(vm, &self.args, inplace),
            "upper" => call::upper(vm, &self.args, inplace),
            "lower" => call::lower(vm, &self.args, inplace),
            "replace" => call::replace(vm, &self.args, inplace, true),
            "replace_one" => call::replace(vm, &self.args, inplace, false),
            "strip" => call::strip(vm, &self.args, false, inplace, true),
            "lstrip" => call::strip(vm, &self.args, false, inplace, true),
            "rstrip" => call::strip(vm, &self.args, inplace, true, false),
            "slice" => call::slice(vm, &self.args, inplace),
            "header" => call::header(vm),
            "cast" => call::cast(vm, &self.args, &sub_function_name, inplace),
            "fill_null" => call::fill(vm, &self.args, inplace),
            "dup" => call::dup(vm, &self.args, UniqueKeepStrategy::First),
            "dup_none" => call::dup(vm, &self.args, UniqueKeepStrategy::None),
            "dup_last" => call::dup(vm, &self.args, UniqueKeepStrategy::Last),
            "dup_any" => call::dup(vm, &self.args, UniqueKeepStrategy::Any),
            "abs" => call::abs(vm, &self.args, inplace),
            "is_null" => call::is_null(vm, &self.args, false),
            "is_not_null" => call::is_null(vm, &self.args, true),
            "is_na" => call::is_na(vm, &self.args, false),
            "is_not_na" => call::is_na(vm, &self.args, true),
            "drop_null" => call::drop_null(vm, &self.args, false),
            "drop_null_" => call::drop_null(vm, &self.args, true),
            "to_fa" | "to_fasta" => call::to_fasta(vm),
            "to_fq" | "to_fastq" => call::to_fastq(vm),
            _ => {
                let simi = compute_similarity(&function_name, vec![]);
                let h = &mut vm.hint;
                h.white("Function `")
                    .cyan(&function_name)
                    .white("` does not found.");

                if simi.is_some() {
                    h.white(" Similar function `")
                        .cyan(simi.unwrap())
                        .white("` found.");
                }
                h.print_and_exit();
            }
        }
    }
}

mod test {}
