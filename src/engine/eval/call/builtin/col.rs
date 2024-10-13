use crate::source::{DataframeSource, SourceType};

use super::*;

pub fn col(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;
    let col_index = match args.first().unwrap() {
        ast::Expr::Constant(n) => n.eval(vm)?,
        ast::Expr::Name(n) => n.eval(vm)?,
        ast::Expr::Call(c) => c.eval(vm)?,
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support column index".to_string(),
            ));
        }
    };

    let c = match col_index {
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

    let mut data_type = None;
    if vm.source.source_type() != SourceType::Dataframe {
        data_type = {
            vm.status
                .selected_columns
                .iter()
                .find(|x| x.name == c)
                .map(|x| x.data_type.clone())
        };
    }

    Ok(value::Value::Column(value::Column {
        col_name: c,
        force: false,
        new: false,
        data_type: data_type,
    }))
}
