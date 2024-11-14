use super::super::*;
use filterx_source::source::SourceType;

pub fn len<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_name = eval_col!(
        vm,
        &args[0],
        "len: expected a column name as first argument"
    );
    let name = col_name.column()?;
    let mut e = col_name.expr()?;
    vm.source_mut().has_column(name);
    // !Note: Only csv has a character length.
    match vm.source_type() {
        SourceType::Csv => {
            e = e.str().len_chars();
        }
        _ => {
            e = e.str().len_bytes();
        }
    }
    Ok(value::Value::named_expr(None, e))
}
