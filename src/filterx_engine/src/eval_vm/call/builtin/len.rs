use super::*;

pub fn len<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_name = eval_col!(vm, &args[0], "len: expected a column name as first argument");
    vm.source.has_column(col_name.column()?);
    let col_name = col_name.expr()?.str();
    let e;
    // !Note: Only csv has a character length.
    if vm.source_type == "csv".into() {
        e = col_name.len_chars();
    } else {
        e = col_name.len_bytes();
    }
    Ok(value::Value::Expr(e))
}
