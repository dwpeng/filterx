use std::ops::Deref;

use polars::frame::UniqueKeepStrategy;

use super::super::ast;

use crate::vm::Vm;
use filterx_core::{value, FilterxResult};
use filterx_source::source::SourceType;

use crate::eval::call::builtin as call;
use crate::eval::Eval;

fn compute_similarity(_target: &str, _reference: Vec<&'static str>) -> Option<&'static str> {
    None
}

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

        match function_name.as_str() {
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
            "print" | "format" | "fmt" => call::print(vm, &self.args),
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
            "fill_null" => call::fill(vm, &self.args, false),
            "fill_null_" => call::fill(vm, &self.args, true),
            "dup" => call::dup(vm, &self.args, UniqueKeepStrategy::First),
            "dup_none" => call::dup(vm, &self.args, UniqueKeepStrategy::None),
            "dup_last" => call::dup(vm, &self.args, UniqueKeepStrategy::Last),
            "dup_any" => call::dup(vm, &self.args, UniqueKeepStrategy::Any),
            "abs" => call::abs(vm, &self.args, false),
            "abs_" => call::abs(vm, &self.args, true),
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
