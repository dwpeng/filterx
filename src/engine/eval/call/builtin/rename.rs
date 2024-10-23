use super::expect_args_len;
pub use crate::engine::ast;
pub use crate::engine::eval::Eval;
pub use crate::engine::value;
pub use crate::engine::vm::Vm;
pub use crate::source::Source;
pub use crate::source::{DataframeSource, SourceType};
pub use crate::{FilterxError, FilterxResult};

pub fn rename(vm: &mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    expect_args_len(args, 2)?;
    let col_value = match args.first().unwrap() {
        ast::Expr::Constant(n) => n.eval(vm)?,
        ast::Expr::Name(n) => n.eval(vm)?,
        ast::Expr::Call(c) => c.eval(vm)?,
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support column index".to_string(),
            ));
        }
    };

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

    let new_col_value = match args.get(1).unwrap() {
        ast::Expr::Constant(n) => n.eval(vm)?,
        ast::Expr::Name(n) => n.eval(vm)?,
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support column index".to_string(),
            ));
        }
    };

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
        _ => {
            return Err(FilterxError::RuntimeError(
                "Only support dataframe.".to_string(),
            ));
        }
    }

    Ok(value::Value::None)
}
