use polars::prelude::{col, lit};

use super::*;

pub fn strip<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
    right: bool,
    left: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 2)?;

    let col_name = eval_col!(
        vm,
        &args[0],
        "strip: expected a column name as first argument"
    );

    let col_name = col_name.column()?;
    vm.source.has_column(&col_name);

    let patt = eval!(
        vm,
        &args[1],
        "strip: expected a string pattern as second argument",
        Constant
    );
    let patt = patt.string()?;
    let patt = lit(patt.as_str());

    let e = match (right, left) {
        (true, true) => col(col_name).str().strip_chars(patt),
        (true, false) => col(col_name).str().strip_suffix(patt),
        (false, true) => col(col_name).str().strip_prefix(patt),
        (false, false) => unreachable!(),
    };

    if inplace {
        vm.source.with_column(e.alias(col_name), None);
        return Ok(value::Value::None);
    }

    Ok(value::Value::Expr(e))
}