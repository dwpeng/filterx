use super::*;

pub fn drop<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    let mut drop_columns = vec![];

    for arg in args {
        let pass = check_types!(arg, Name, Call);
        if !pass {
            let h = &mut vm.hint;
            h.white("drop: expected a column name as argument")
                .print_and_exit();
        }
    }

    for arg in args {
        let col = eval!(vm, arg, Name, Call, UnaryOp);
        drop_columns.push(col.text()?);
    }

    vm.source.drop(drop_columns);
    Ok(value::Value::None)
}
