pub use super::*;
pub use crate::engine::ast;
pub use crate::engine::eval::Eval;
pub use crate::engine::value;
pub use crate::engine::vm::Vm;
pub use crate::source::DataframeSource;
pub use crate::source::Source;
pub use crate::{FilterxError, FilterxResult};

pub fn expect_args_len(
    args: &Vec<crate::engine::ast::Expr>,
    len: usize,
) -> crate::FilterxResult<()> {
    if args.len() != len {
        return Err(crate::FilterxError::RuntimeError(format!(
            "Expect {} args, but got {} args.",
            len,
            args.len()
        )));
    }
    Ok(())
}

#[macro_export]
macro_rules! eval {
    ($vm:expr, $target:expr, $msg:literal, $($expr:ident),*) => {
        match $target {
            $(
                ast::Expr::$expr(x) => x.eval($vm)?,
            )*
            _ => {
                return Err(FilterxError::RuntimeError($msg.to_string()));
            }
        }
    };
}

macro_rules! builtin_function {
    ($($name:ident),*) => {
        pub static BUILTIN_FUNCTIONS: &'static [&'static str] = &[
            $(
                stringify!($name),
            )*
        ];
        $(
            #[allow(non_snake_case)]
            pub mod $name;
            pub use $name::$name;
        )*
    };

    ($($name:ident,)*) => {
        pub static BUILTIN_FUNCTIONS: &'static [&'static str] = &[
            $(
                stringify!($name),
            )*
        ];
        $(
            #[allow(non_snake_case)]
            pub mod $name;
            pub use $name::$name;
        )*
    };
}

builtin_function! {
    alias,
    col,
    drop,
    select,
    print,
    rename,
    head,
    tail,
    sort,
    len,
    limit,
    gc,
    rev,
    revcomp,
    upper,
    lower,
    replace,
    strip,
    slice,
    header,
    cast,
    fill,
}
