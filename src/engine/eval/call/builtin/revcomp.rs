use std::borrow::Cow;

use super::*;
use crate::eval;
use crate::FilterxError;

use polars::error::PolarsResult;
use polars::prelude::col;
use polars::prelude::ChunkApply;
use polars::prelude::GetOutput;
use polars::series::IntoSeries;
use polars::series::Series;

fn compute_revcomp(s: Series) -> PolarsResult<Option<Series>> {
    let ca = s.str()?;
    let ca = ca.apply_values(|s| {
        let s = s.chars().rev().collect::<String>();
        let s = s
            .chars()
            .map(|c| match c {
                'A' => 'T',
                'T' => 'A',
                'C' => 'G',
                'G' => 'C',
                'a' => 't',
                't' => 'a',
                'c' => 'g',
                'g' => 'c',
                _ => c,
            })
            .collect::<String>();
        Cow::Owned(s)
    });
    Ok(Some(ca.into_series()))
}

pub fn revcomp<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_name = eval!(
        vm,
        &args[0],
        "rev: expected a Series as first argument",
        Name,
        Call
    );
    let col_name = match col_name {
        value::Value::Column(c) => c.col_name.to_string(),
        _ => {
            return Err(FilterxError::RuntimeError(
                "rev: need a column name".to_string(),
            ))
        }
    };
    let e = col(&col_name).map(compute_revcomp, GetOutput::same_type());
    if inplace {
        let lazy = vm.source.dataframe_mut_ref().unwrap().lazy.clone();
        let lazy = lazy.with_column(e.clone().alias(&col_name));
        vm.source.dataframe_mut_ref().unwrap().update(lazy);
        return Ok(value::Value::None);
    }
    return Ok(value::Value::Expr(e));
}
