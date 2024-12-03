use polars::prelude::lit;

use super::super::*;

pub fn extract<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 2)?;

    let col_name = eval_col!(
        vm,
        &args[0],
        "extract: expected a column name as first argument"
    );
    let name = col_name.column()?;
    let e = col_name.expr()?;
    vm.source_mut().has_column(name);

    let patt = eval!(
        vm,
        &args[1],
        "extract: expected a constant pattern and replacement as second argument",
        Constant
    );
    let patt = patt.string()?;
    let patt = lit(patt.as_str());

    let e = e.str().extract(patt, 1);
    if inplace {
        vm.source_mut().with_column(e.alias(name), None);
        return Ok(value::Value::None);
    }
    Ok(value::Value::named_expr(Some(name.to_string()), e))
}
