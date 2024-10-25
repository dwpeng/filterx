use super::*;
use crate::eval;
use crate::FilterxError;

use polars::prelude::col;

pub fn rev<'a>(
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
    let e = col(&col_name).str().reverse();
    if inplace {
        let lazy = vm.source.dataframe_mut_ref().unwrap().lazy.clone();
        let lazy = lazy.with_column(e.clone().alias(&col_name));
        vm.source.dataframe_mut_ref().unwrap().update(lazy);
        return Ok(value::Value::None);
    }
    return Ok(value::Value::Expr(e));
}
