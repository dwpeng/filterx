use super::super::*;

pub fn rename(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 2)?;

    let old_col = eval_col!(
        vm,
        &args[0],
        "rename: expected a column name as first argument"
    );
    let old_col = old_col.column()?;
    vm.source_mut().has_column(old_col);
    let new_col_value = eval_col!(
        vm,
        &args[1],
        "rename: expected a column name as second argument"
    );
    if vm
        .source_mut()
        .ret_column_names
        .contains(&new_col_value.column()?.to_string())
    {
        return Ok(value::Value::None);
    }
    let new_col = new_col_value.column()?;
    vm.source_mut().rename([old_col], [new_col]);
    Ok(value::Value::None)
}
