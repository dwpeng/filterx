pub use super::*;
pub use crate::engine::ast;
pub use crate::engine::eval::Eval;
pub use crate::engine::value;
pub use crate::engine::vm::Vm;
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
    Alias,
    alias,
    col,
    drop,
    select,
    row,
    print,
}
