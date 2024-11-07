pub mod ast;
pub mod eval;
pub mod value;
pub mod vm;

#[macro_export]
macro_rules! check_types {
    ($target: expr, $($types:ident),*) => {
        match $target {
            $(
                ast::Expr::$types(_) => {
                    true
                }
            )*
            _ => {
                false
            }
        }
    };
    ($target: expr, $($types:ident),*,) => {
        match $target {
            $(
                ast::Expr::$types(_) => {
                    true
                }
            )*
            _ => {
                false
            }
        }
    };
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

    ($vm:expr, $target:expr, $($expr:ident),*) => {
        match $target {
            $(
                ast::Expr::$expr(x) => x.eval($vm)?,
            )*
            _ => {
                unreachable!();
            }
        }
    };
}
