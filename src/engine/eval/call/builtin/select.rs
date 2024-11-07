use crate::util::check_repeat;

use super::*;

pub fn select<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    let mut select_dolumns = vec![];

    for arg in args {
        let pass = check_types!(arg, Name, Call);
        if !pass {
            let h = &mut vm.hint;
            h.white("select: expected a column name as first argument")
                .print_and_exit();
        }
    }

    for arg in args {
        let col = eval!(vm, arg, Name, Call);
        let col = match col {
            value::Value::Item(c) => c.col_name,
            value::Value::Name(c) => c.name,
            _ => {
                let h = &mut vm.hint;
                h.white("select: expected a column name as first argument")
                    .print_and_exit();
            }
        };

        select_dolumns.push(col);
    }

    if check_repeat(&select_dolumns) {
        let h = &mut vm.hint;
        h.white("select: don't support duplicate column, but got duplicate column: ")
            .cyan(&select_dolumns.join(", "))
            .print_and_exit();
    }

    vm.source.select(select_dolumns);
    Ok(value::Value::None)
}
