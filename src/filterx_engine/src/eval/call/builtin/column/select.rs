use crate::util::check_repeat;

use super::super::*;

pub fn select<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    let mut select_dolumns = vec![];

    for arg in args {
        let col = eval_col!(vm, arg, "select: expected a column name as first argument");
        let col = col.column()?;
        vm.source_mut().has_column(col);
        select_dolumns.push(col.into());
    }

    if check_repeat(&select_dolumns) {
        let h = &mut vm.hint;
        h.white("select: don't support duplicate column, but got duplicate column: ")
            .cyan(&select_dolumns.join(", "))
            .print_and_exit();
    }

    vm.source_mut().select(select_dolumns);
    Ok(value::Value::None)
}
