use super::super::*;

pub fn col(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_value = eval_col!(
        vm,
        &args[0],
        "col only support column index, column name, or function which return a column name."
    );
    let c = match col_value {
        value::Value::Int(i) => vm.source_mut().index2column(i as usize),
        value::Value::Str(s) => s,
        value::Value::Name(c) => c.name,
        value::Value::Item(c) => c.col_name,
        _ => {
            let h = &mut vm.hint;
            h.white("col only support column index, column name, or function which return a column name.").print_and_exit();
        }
    };
    vm.source_mut().has_column(&c);
    Ok(value::Value::Item(value::Item {
        col_name: c,
        data_type: None,
    }))
}
