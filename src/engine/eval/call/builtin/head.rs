use super::*;

pub fn head(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let pass = check_types!(&args[0], Constant, UnaryOp);
    if !pass {
        let h = &mut vm.hint;
        h.white("head: expected a non-negative number as first argument")
            .print_and_exit();
    }
    let n = eval!(vm, &args[0], Constant, UnaryOp);
    let nrow = match n {
        value::Value::Int(i) => {
            if i >= 0 {
                i as usize
            } else {
                let h = &mut vm.hint;
                h.white("head: expected a non-negative number as first argument, but got ")
                    .cyan(&format!("{}", i))
                    .print_and_exit();
            }
        }
        _ => {
            let h = &mut vm.hint;
            h.white("head: expected a non-negative number as first argument")
                .print_and_exit();
        }
    };

    vm.source.slice(0, nrow);
    vm.status.limit = nrow;
    Ok(value::Value::None)
}
