use super::*;

pub fn col(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let col_value = eval!(
        vm,
        &args[0],
        "Only support column index",
        Constant,
        Name,
        Call
    );

    let c = match col_value {
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
                "col only support column index or column name".to_string(),
            ));
        }
    };

    let data_type = None;
    Ok(value::Value::Column(value::Column {
        col_name: c,
        force: false,
        new: false,
        data_type: data_type,
    }))
}
