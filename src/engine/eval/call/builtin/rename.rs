use super::*;

pub fn rename(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 2)?;

    let pass = check_types!(&args[0], Constant, Name, Call);
    if !pass {
        let h = &mut vm.hint;
        h.white("rename: expected a column name as first argument")
            .print_and_exit();
    }

    let col_value = eval!(vm, &args[0], Constant, Name, Call);

    let old_col = match col_value {
        value::Value::Int(i) => {
            if i >= 0 {
                DataframeSource::index2column(i as usize)
            } else {
                let h = &mut vm.hint;
                h.white("while using `col` function, column index should be positive integer and start from 1.").print_and_exit();
            }
        }
        value::Value::Str(s) => s,
        value::Value::Column(c) => c.col_name,
        value::Value::Name(c) => c.name,
        _ => {
            let h = &mut vm.hint;
            h.white("rename only support column index, column name, or function which return a column name.").print_and_exit();
        }
    };

    let pass = check_types!(&args[1], Name, Call);
    if !pass {
        let h = &mut vm.hint;
        h.white("rename: expected a column name as second argument")
            .print_and_exit();
    }

    let new_col_value = eval!(vm, &args[1], Name, Call);

    let new_col = match new_col_value {
        value::Value::Str(s) => s,
        value::Value::Column(c) => c.col_name,
        value::Value::Name(n) => n.name,
        _ => {
            let h = &mut vm.hint;
            h.white("only support column name for new name in `rename`").print_and_exit();
        }
    };

    vm.source.rename([old_col], [new_col]);
    Ok(value::Value::None)
}
