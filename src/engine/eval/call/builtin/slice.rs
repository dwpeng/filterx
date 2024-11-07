use polars::prelude::{col, lit};

use super::*;

fn check_number<'a>(vm: &'a mut Vm, n: &ast::Expr) -> FilterxResult<u32> {
    let pass = check_types!(n, Constant);
    if !pass {
        let h = &mut vm.hint;
        h.white("head: expected a non-negative number as argument")
            .print_and_exit();
    }
    let n = eval!(vm, n, Constant);

    match n {
        value::Value::Int(i) => {
            if i >= 0 {
                Ok(i as u32)
            } else {
                let h = &mut vm.hint;
                h.white("head: expected a non-negative number as argument, but got ")
                    .cyan(&format!("{}", i))
                    .print_and_exit();
            }
        }
        _ => {
            let h = &mut vm.hint;
            h.white("head: expected a non-negative number as argument")
                .print_and_exit();
        }
    }
}

pub fn slice<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    let pass = check_types!(&args[0], Name, Call, UnaryOp);
    if !pass {
        let h = &mut vm.hint;
        h.white("slice: expected a column name as first argument")
            .print_and_exit();
    }
    let col_name = eval!(vm, &args[0], Name, Call, UnaryOp);
    let col_name = match col_name {
        value::Value::Name(n) => n.name,
        value::Value::Column(c) => c.col_name,
        _ => {
            let h = &mut vm.hint;
            h.white("slice: expected a column name as first argument")
                .print_and_exit();
        }
    };
    let length;
    let mut start = 0;
    if args.len() == 2 {
        length = check_number(vm, &args[1])?;
    } else {
        start = check_number(vm, &args[1])?;
        length = check_number(vm, &args[2])?;
    }

    let e = col(&col_name).str().slice(lit(start), lit(length));

    if inplace {
        vm.source.with_column(e.alias(&col_name));
        return Ok(value::Value::None);
    }

    Ok(value::Value::Expr(e))
}
