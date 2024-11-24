use polars::prelude::lit;

use super::super::*;

fn check_number<'a>(vm: &'a mut Vm, n: &ast::Expr) -> FilterxResult<u32> {
    let n = eval_int!(vm, n, "head: expected a non-negative number as argument");
    let n = n.int()?;
    if n > 0 {
        return Ok(n as u32);
    } else {
    }
    let h = &mut vm.hint;
    h.white("head: expected a non-negative number as argument")
        .print_and_exit();
}

pub fn slice<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    let col_name = eval_col!(
        vm,
        &args[0],
        "slice: expected a column name as first argument"
    );
    let name = col_name.column()?;
    let e = col_name.expr()?;
    vm.source_mut().has_column(name);
    let length;
    let mut start = 0;
    if args.len() == 2 {
        length = check_number(vm, &args[1])?;
    } else {
        start = check_number(vm, &args[1])?;
        if start > 0 {
            start -= 1;
        }
        length = check_number(vm, &args[2])?;
    }

    let e = e.str().slice(lit(start), lit(length));

    if inplace {
        vm.source_mut().with_column(e.alias(name), None);
        return Ok(value::Value::None);
    }

    Ok(value::Value::named_expr(Some(name.to_string()), e))
}
