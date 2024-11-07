use polars::prelude::col;

use super::*;

pub fn lower<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let pass = check_types!(&args[0], Name, Call);
    if !pass {
        let h = &mut vm.hint;
        h.white("lower: expected a column name as first argument")
            .print_and_exit();
    }

    let col_name = eval!(vm, &args[0], Name, Call);
    let col_name = match col_name {
        value::Value::Item(c) => c.col_name,
        value::Value::Name(n) => n.name,
        _ => {
            let h = &mut vm.hint;
            h.white("lower: expected a column name as first argument")
                .print_and_exit();
        }
    };

    if inplace {
        vm.source
            .with_column(col(&col_name).str().to_lowercase().alias(&col_name));
        return Ok(value::Value::None);
    }

    Ok(value::Value::Expr(col(col_name).str().to_lowercase()))
}
