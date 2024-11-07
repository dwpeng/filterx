use super::*;
use polars::prelude::Expr;
use value::Value;

pub fn fill<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 2)?;

    let pass = check_types!(&args[0], Name, Call);
    if !pass {
        let h = &mut vm.hint;
        h.white("fill: expected a column name as first argument")
            .print_and_exit();
    }

    let pass = check_types!(&args[1], Constant);
    if !pass {
        let h = &mut vm.hint;
        h.white("fill: expected a constant value as second argument")
            .print_and_exit();
    }

    let col_name = eval!(vm, &args[0], Name, Call);
    let col_name = col_name.expr()?;
    let const_value = eval!(vm, &args[1], Constant).expr()?;
    match const_value {
        Expr::Literal(_) => {}
        _ => {
            let h = &mut vm.hint;
            h.white("fill: expected a constant value as second argument")
                .print_and_exit();
        }
    }
    let e = col_name.fill_null(const_value);

    if inplace {
        let lazy = &mut vm.source;
        lazy.with_column(e);
        return Ok(Value::None);
    }
    Ok(Value::Expr(e))
}
