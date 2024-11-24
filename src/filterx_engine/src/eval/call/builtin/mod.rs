pub use super::*;
pub use crate::ast;
pub use crate::eval::Eval;
pub use crate::vm::Vm;
pub use crate::{eval, eval_col, eval_int, execuable};
pub use filterx_core::{value, FilterxError, FilterxResult};
pub use filterx_source::{source::SourceType, DataframeSource};

pub fn expect_args_len(args: &Vec<crate::ast::Expr>, len: usize) -> FilterxResult<()> {
    if args.len() != len {
        return Err(FilterxError::RuntimeError(format!(
            "Expect {} args, but got {} args.",
            len,
            args.len()
        )));
    }
    Ok(())
}

pub mod string;
pub use string::*;

pub mod sequence;
pub use sequence::*;

pub mod column;
pub use column::*;

pub mod number;
pub use number::*;

pub mod row;
pub use row::*;
