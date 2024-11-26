use super::super::*;
use polars::prelude::col as polars_col;

pub fn col(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_value = eval_col!(
        vm,
        &args[0],
        "col only support column index, column name, or function which return a column name."
    );
    let c = match &col_value {
        value::Value::Int(i) => vm.source_mut().index2column(*i as usize),
        value::Value::Str(s) => s.to_owned(),
        value::Value::Name(c) => c.name.to_owned(),
        value::Value::NamedExpr(_) => return Ok(col_value.clone()),
        _ => {
            let h = &mut vm.hint;
            h.white("col only support column index, column name, or function which return a column name.").print_and_exit();
        }
    };
    Ok(value::Value::named_expr(Some(c.clone()), polars_col(c)))
}
