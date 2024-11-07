use super::*;

pub fn col(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let pass = check_types!(&args[0], Constant, Name, Call);

    if !pass {
        let h = &mut vm.hint;
        h.white(
            "col only support column index, column name, or function which return a column name.",
        )
        .print_and_exit();
    }
    let col_value = eval!(vm, &args[0], Constant, Name, Call);

    let c = match col_value {
        value::Value::Int(i) => {
            if i >= 0 {
                vm.source.index2column(i as usize)
            } else {
                let h = &mut vm.hint;
                h.white("while using `col` function, column index should be positive integer and start from 1.").print_and_exit();
            }
        }
        value::Value::Str(s) => s,
        value::Value::Name(c) => c.name,
        value::Value::Item(c) => c.col_name,
        _ => {
            let h = &mut vm.hint;
            h.white("col only support column index, column name, or function which return a column name.").print_and_exit();
        }
    };

    Ok(value::Value::Item(value::Item {
        col_name: c,
        data_type: None,
    }))
}
