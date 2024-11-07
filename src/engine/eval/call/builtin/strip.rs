use polars::prelude::{col, lit};

use super::*;

pub fn strip<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
    right: bool,
    left: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 2)?;

    let pass = check_types!(&args[0], Name, Call);
    if !pass {
        let h = &mut vm.hint;
        h.white("strip: expected a column name as first argument")
            .print_and_exit();
    }

    let col_name = eval!(
        vm,
        &args[0],
        Name,
        Call,
        UnaryOp
    );

    let col_name = match col_name {
        value::Value::Column(c) => c.col_name,
        value::Value::Name(n) => n.name,
        _ => {
            let h = &mut vm.hint;
            h.white("strip: expected a column name as first argument")
                .print_and_exit();
        }
    };

    let pass = check_types!(&args[1], Constant);
    if !pass {
        let h = &mut vm.hint;
        h.white("strip: expected a string pattern as second argument")
            .print_and_exit();
    }

    let patt = eval!(vm, &args[1], Constant);
    let patt = patt.string()?;
    let patt = lit(patt.as_str());

    let e = match (right, left) {
        (true, true) => col(&col_name).str().strip_chars(patt),
        (true, false) => col(&col_name).str().strip_suffix(patt),
        (false, true) => col(&col_name).str().strip_prefix(patt),
        (false, false) => unreachable!(),
    };

    if inplace {
        vm.source
            .dataframe_mut_ref()
            .unwrap()
            .with_column(e.alias(&col_name));
        return Ok(value::Value::None);
    }

    Ok(value::Value::Expr(e))
}
