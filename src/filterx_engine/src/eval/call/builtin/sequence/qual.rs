use super::super::*;
use filterx_source::QualityType;
use polars::prelude::*;

use polars_arrow::{
    array::{ArrayRef, Float32Array, Utf8ViewArray},
    buffer::Buffer,
    datatypes::ArrowDataType,
};

// copy from https://github.com/shenwei356/bio/blob/master/seq/seq.go

static mut QUAL_MAP: [f32; 256] = [0.0; 256];

fn init_qual_map() {
    for i in 0..256 {
        unsafe {
            // Q = -10 * log10(p)
            // p: rate of base error, p = 10 ^ (-Q / 10)
            // Q: quality score, use i representing 0-255
            // phred33: ASCII 33 + Q
            // phred64: ASCII 64 + Q
            // so, while computing the probability, we need to minus 33 or 64 to get original phred score as Q
            QUAL_MAP[i] = f32::powf(10.0, i as f32 / -10.0);
        }
    }
}

fn compute_qual_phred33_kernel(array: &Utf8ViewArray) -> ArrayRef {
    let array = array
        .values_iter()
        .map(|seq| {
            let length = seq.len();
            if length == 0 {
                return 0.0;
            }
            let max = seq.bytes().max().unwrap();
            let min = seq.bytes().min().unwrap();
            if max > 127 || min < 33 {
                return 0.0;
            }
            let sum_qual: f32 = seq
                .bytes()
                .map(|b| unsafe {
                    let b = b as usize - 33;
                    QUAL_MAP[b]
                })
                .sum();
            if sum_qual == 0.0 {
                return 0.0;
            }
            -10.0 * (sum_qual / seq.len() as f32).log10()
        })
        .collect::<Vec<_>>();
    let values: Buffer<_> = array.into();
    let array = Float32Array::new(ArrowDataType::Float32, values, None);
    Box::new(array)
}

fn compute_qual_phred64_kernel(array: &Utf8ViewArray) -> ArrayRef {
    let array = array
        .values_iter()
        .map(|seq| {
            let length = seq.len();
            if length == 0 {
                return 0.0;
            }
            let max = seq.bytes().max().unwrap();
            let min = seq.bytes().min().unwrap();
            if max > 127 || min < 33 {
                return 0.0;
            }
            let qual_sum: f32 = seq
                .bytes()
                .map(|c| unsafe {
                    let c = c as usize - 64;
                    QUAL_MAP[c] as f32
                })
                .sum();
            if qual_sum == 0.0 {
                return 0.0;
            }
            -10.0 * (qual_sum / seq.len() as f32).log10()
        })
        .collect::<Vec<_>>();
    let values: Buffer<_> = array.into();
    let array = Float32Array::new(ArrowDataType::Float32, values, None);
    Box::new(array)
}

fn compute_qual_phred64(s: Column) -> PolarsResult<Option<Column>> {
    if !s.dtype().is_string() {
        return Err(PolarsError::InvalidOperation(
            format!(
                "Computing quality needs a string column, got column `{}` with type `{}`",
                s.name(),
                s.dtype()
            )
            .into(),
        ));
    }
    init_qual_map();
    let s = s.as_materialized_series();
    let s = s.str()?.as_string();
    let c = s
        .apply_kernel_cast::<Float32Type>(&compute_qual_phred64_kernel)
        .into_column();
    Ok(Some(c))
}

fn compute_qual_phred33(s: Column) -> PolarsResult<Option<Column>> {
    if !s.dtype().is_string() {
        return Err(PolarsError::InvalidOperation(
            format!(
                "Computing quality needs a string column, got column `{}` with type `{}`",
                s.name(),
                s.dtype()
            )
            .into(),
        ));
    }
    init_qual_map();
    let s = s.as_materialized_series();
    let s = s.str()?.as_string();
    let c = s
        .apply_kernel_cast::<Float32Type>(&compute_qual_phred33_kernel)
        .into_column();
    Ok(Some(c))
}

pub fn qual<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    if !vm.source.source_type.is_fastq() {
        let h = &mut vm.hint;
        h.white("qual: Only available on fastq source")
            .print_and_exit();
    }
    let col_name = eval_col!(
        vm,
        &args[0],
        "qual: expected a column name as first argument"
    );
    let qtype = vm.source.get_fastq()?;
    let name = col_name.column()?;
    let mut e = col_name.expr()?;
    vm.source().has_column(name);
    match qtype.quality_type {
        QualityType::Phred33 => {
            e = e.map(compute_qual_phred33, GetOutput::float_type());
        }
        QualityType::Phred64 => {
            e = e.map(compute_qual_phred64, GetOutput::float_type());
        }
        QualityType::Auto => {
            let h = &mut vm.hint;
            h.white("qual: Unable to detect quality type")
                .print_and_exit();
        }
    };
    return Ok(value::Value::named_expr(None, e));
}
