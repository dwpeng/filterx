use super::super::*;
use polars::prelude::*;
use std::borrow::Cow;

static mut STRING_WIDTH: usize = 0;

fn compute_width_string(s: Column) -> PolarsResult<Option<Column>> {
    let ca = s.str()?;
    let ca = ca.apply_values(|s| {
        let s = s.as_bytes();
        // compute new length
        let length = s.len();
        let width = unsafe { STRING_WIDTH };

        if length <= width {
            return Cow::Owned(String::from_utf8_lossy(s).into_owned());
        }

        // re-layout the string to the new string
        // Repear unit: width-chars + \n
        let niter = length / width;
        let left = length % width;
        let mut new_string = Vec::with_capacity(niter + width);
        let mut offset = 0;
        for _ in 0..niter {
            let start = offset;
            offset += width;
            let end = offset;
            let sub = &s[start..end];
            new_string.extend_from_slice(sub);
            new_string.push(b'\n');
        }
        if left > 0 {
            let start = offset;
            offset += left;
            let end = offset;
            let sub = &s[start..end];
            new_string.extend_from_slice(sub);
        }else{
            new_string.pop();
        }
        let s = unsafe { String::from_utf8_unchecked(new_string) };
        Cow::Owned(s)
    });
    Ok(Some(ca.into_column()))
}

pub fn width<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 2)?;

    let col_name = eval_col!(
        vm,
        &args[0],
        "width: expected a column name as first argument"
    );

    let width = eval!(
        vm,
        &args[1],
        "width: expected an integer as second argument",
        Constant
    );

    let width = width.u32()?;

    unsafe { STRING_WIDTH = width as usize };

    let name = col_name.column()?;
    let e = col_name.expr()?;
    vm.source_mut().has_column(name);
    let e = e.map(compute_width_string, GetOutput::same_type());

    if inplace {
        vm.source_mut().with_column(e.alias(name), None);
        return Ok(value::Value::None);
    }
    Ok(value::Value::named_expr(Some(name.to_string()), e))
}
