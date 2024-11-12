use value::NamedExpr;

use super::*;

pub fn lower<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_name = eval_col!(
        vm,
        &args[0],
        "lower: expected a column name as first argument"
    );
    let name = col_name.column()?;
    let e = col_name.expr()?;
    vm.source.has_column(name);
    if inplace {
        vm.source
            .with_column(e.str().to_lowercase().alias(name), None);
        return Ok(value::Value::None);
    }

    let ne = NamedExpr {
        name: Some(name.to_string()),
        expr: e.str().to_lowercase().alias(name),
    };

    Ok(value::Value::NamedExpr(ne))
}
