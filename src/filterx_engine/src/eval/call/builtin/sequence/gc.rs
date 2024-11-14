use super::super::*;
use filterx_source::FastaRecordType;
use polars::prelude::*;

use polars_arrow::{
    array::{ArrayRef, Float32Array, Utf8ViewArray},
    buffer::Buffer,
    datatypes::ArrowDataType,
};

fn compute_gc_kernel(array: &Utf8ViewArray) -> ArrayRef {
    let array = array
        .values_iter()
        .map(|seq| {
            let length = seq.len();
            if length == 0 {
                return 0.0;
            }
            let gc = seq
                .bytes()
                .filter(|c| *c == b'G' || *c == b'C' || *c == b'c' || *c == b'g')
                .count();
            if gc == 0 {
                return 0.0;
            }
            (gc as f32) / (length as f32)
        })
        .collect::<Vec<_>>();
    let values: Buffer<_> = array.into();
    let array = Float32Array::new(ArrowDataType::Float32, values, None);
    Box::new(array)
}

fn compute_gc(s: Column) -> PolarsResult<Option<Column>> {
    if !s.dtype().is_string() {
        return Err(PolarsError::InvalidOperation(
            format!(
                "Computing GC content needs a string column, got column `{}` with type `{}`",
                s.name(),
                s.dtype()
            )
            .into(),
        ));
    }
    let s = s.as_materialized_series();
    let s = s.str()?.as_string();
    let c = s
        .apply_kernel_cast::<Float32Type>(&compute_gc_kernel)
        .into_column();
    Ok(Some(c))
}

pub fn gc<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    if vm.source.source_type.is_fasta() || vm.source.source_type.is_fastq() {
        if vm.source.source_type.is_fasta() {
            let fasta = vm.source.get_fasta()?;
            match fasta.record_type {
                FastaRecordType::Protein => {
                    let h = &mut vm.hint;
                    h.white("gc: protein sequence is not supported")
                        .print_and_exit();
                }
                _ => {}
            }
        }
    }
    let col_name = eval_col!(vm, &args[0], "gc: expected a column name as first argument");
    let name = col_name.column()?;
    let e = col_name.expr()?;
    vm.source_mut().has_column(name);
    let e = e.map(compute_gc, GetOutput::float_type());
    return Ok(value::Value::named_expr(None, e));
}
