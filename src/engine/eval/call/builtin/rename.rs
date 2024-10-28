use super::*;

pub fn rename(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 2)?;

    let col_value = eval!(
        vm,
        &args[0],
        "Only support column index",
        Constant,
        Name,
        Call
    );

    let old_col = match col_value {
        value::Value::Int(i) => {
            if i >= 0 {
                DataframeSource::index2column(i as usize)
            } else {
                return Err(FilterxError::RuntimeError(
                    "Index starts from 1".to_string(),
                ));
            }
        }
        value::Value::Str(s) => s,
        value::Value::Column(c) => c.col_name,
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support column index".to_string(),
            ));
        }
    };

    let new_col_value = eval!(
        vm,
        &args[1],
        "Only support column index",
        Constant,
        Name,
        Call
    );

    let new_col = match new_col_value {
        value::Value::Str(s) => s,
        value::Value::Column(c) => c.col_name,
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support column index".to_string(),
            ));
        }
    };

    match &mut vm.source {
        Source::Dataframe(ref mut df_source) => {
            let lazy = df_source.lazy.clone();
            let lazy = lazy.rename([old_col], [new_col]);
            df_source.lazy = lazy;
        }
    }

    Ok(value::Value::None)
}
