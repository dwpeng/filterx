#[macro_export]
macro_rules! eval {
    ($vm:expr, $target:expr, $msg:literal, $($expr:ident),*) => {
        match $target {
            $(
                ast::Expr::$expr(x) => x.eval($vm)?,
            )*
            _ => {
                $vm.hint.white($msg).print_and_exit();
            }
        }
    };
    ($vm:expr, $target:expr, $msg:expr, $($expr:ident),*) => {
        match $target {
            $(
                ast::Expr::$expr(x) => x.eval($vm)?,
            )*
            _ => {
                $msg.print_and_exit()
            }
        }
    };
}

#[macro_export]
macro_rules! eval_col {
    ($vm:expr, $target:expr, $msg:literal) => {
        eval!($vm, $target, $msg, Name, Call, Constant)
    };
    ($vm:expr, $target:expr, $msg:literal,) => {
        eval!($vm, $target, $msg, Name, Call, Constant)
    };
}

pub struct Builtin {
    pub name: &'static str,
    pub alias: &'static [&'static str],
    pub can_expression: bool,
}

#[macro_export]
macro_rules! builtin_function {
    (   $group: ident,
        $(
            ($name:ident, $expression:expr $(, ($($alias:ident),*))?),
        )*
    ) => {
        pub use crate::engine_macro::Builtin;
        pub static $group: &'static [Builtin] = &[
            $(
                Builtin {
                    name: stringify!($name),
                    alias: &[stringify!($name), $(
                        $(stringify!($alias)),*
                    )?],
                    can_expression: $expression,
                },
            )*
        ];
        $(
            pub mod $name;
            pub use $name::$name;
        )*
    };
}

#[macro_export]
macro_rules! execuable {
    ($vm:expr, $target: literal) => {
        use crate::vm::VmMode;
        if $vm.mode == VmMode::Printable {
            let h = &mut $vm.hint;
            h.white("Con't use ")
                .red($target)
                .white(" in builtin function")
                .green(" `print`.")
                .print_and_exit()
        }
    };
}
