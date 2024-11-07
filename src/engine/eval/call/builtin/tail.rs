use super::*;

pub fn tail(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let pass = check_types!(&args[0], Constant);
    if !pass {
        let h = &mut vm.hint;
        h.white("tail: expected a non-negative number as first argument")
            .print_and_exit();
    }

    let n = eval!(vm, &args[0], Constant, UnaryOp);

    let nrow = match n {
        value::Value::Int(i) => {
            if i >= 0 {
                i as usize
            } else {
                let h = &mut vm.hint;
                h.white("tail: expected a non-negative number as first argument, but got ")
                    .cyan(&format!("{}", i))
                    .print_and_exit();
            }
        }
        _ => {
            let h = &mut vm.hint;
            h.white("tail: expected a non-negative number as first argument")
                .print_and_exit();
        }
    };

    let lazy = vm.source.lazy().tail(nrow as u32);
    vm.source.update(lazy);
    Ok(value::Value::None)
}
