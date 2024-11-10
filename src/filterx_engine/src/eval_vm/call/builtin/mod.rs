pub use super::*;
pub use crate::ast;
pub use crate::eval_vm::Eval;
pub use crate::vm::Vm;
pub use crate::vm::VmSourceType;
pub use crate::{eval, eval_col};
pub use filterx_core::{value, FilterxError, FilterxResult};
pub use filterx_source::Source;

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
    dup,
    abs,
    is_null,
    is_na,
    nr,
    drop_null,
    to_fasta,
    to_fastq,
}
