pub use super::*;
pub use crate::engine::ast;
pub use crate::engine::eval::call::expect_args_len;
pub use crate::engine::eval::Eval;
pub use crate::engine::value;
pub use crate::engine::vm::Vm;
pub use crate::{FilterxError, FilterxResult};

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
}

builtin_function! {
    Alias,
    alias,
    col,
    drop,
    select,
    row
}
