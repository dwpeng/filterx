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

    let pass = check_types!(&args[0], Name, Call);
    if !pass {
        return Err(FilterxError::RuntimeError(
            "rev: expected a column name as first argument".to_string(),
        ));
    }

    let col_name = eval!(
        vm,
        &args[0],
        Name,
        Call
    );
    let col_name = match col_name {
        value::Value::Item(c) => c.col_name.to_string(),
        value::Value::Name(n) => n.name,
        _ => {
            let h = &mut vm.hint;
            h.white("rev: expected a column name as first argument")
                .print_and_exit();
        }
    };
    let e = col(&col_name).str().reverse();
    if inplace {
        vm.source.with_column(e.clone().alias(&col_name), None);
        return Ok(value::Value::None);
    }
    return Ok(value::Value::Expr(e));
}
