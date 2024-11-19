use polars::prelude::Literal;

use super::super::*;

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

    let name = col_name.column()?;
    let e = col_name.expr()?;
    vm.source_mut().has_column(&name);

    let patt = eval!(
        vm,
        &args[1],
        "strip: expected a string pattern as second argument",
        Constant
    );
    let patt = patt.string()?;
    let patt = patt.as_str().lit();

    let e = match (right, left) {
        (true, true) => e.str().strip_chars(patt),
        (true, false) => e.str().strip_chars_end(patt),
        (false, true) => e.str().strip_chars_start(patt),
        (false, false) => unreachable!(),
    };

    if inplace {
        vm.source_mut().with_column(e.alias(name), None);
        return Ok(value::Value::None);
    }

    Ok(value::Value::named_expr(Some(name.to_string()), e))
}
