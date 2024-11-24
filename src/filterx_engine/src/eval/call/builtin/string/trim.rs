use super::super::*;
use polars::prelude::*;

pub fn trim<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 3)?;

    let col_name = eval_col!(
        vm,
        &args[0],
        "trim: expected a column name as first argument"
    );
    let name = col_name.column()?;
    let e = col_name.expr()?;
    vm.source_mut().has_column(name);

    let start = eval_int!(
        vm,
        &args[1],
        "trim: expected a string as second argument"
    );

    let end = eval_int!(
        vm,
        &args[2],
        "trim: expected a string as third argument"
    );

    let start = start.int()?;
    let mut end = end.int()?;

    if start < 0 || end < 0 {
        let h = &mut vm.hint;
        h.white("trim: expected a non-negative number as argument")
            .print_and_exit();
    }

    end += 1;

    let elen;
    if vm.source.source_type == SourceType::Csv {
        elen = e.clone().str().len_chars();
    } else {
        elen = e.clone().str().len_bytes();
    }
    let e = e.str().slice(start.lit(), elen - end.lit());
    if inplace {
        vm.source_mut().with_column(e.clone().alias(name), None);
        return Ok(value::Value::None);
    }
    return Ok(value::Value::named_expr(Some(name.to_string()), e));
}
