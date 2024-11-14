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

#[macro_export]
macro_rules! builtin_function {
    ($($name:ident),*) => {
        $(
            #[allow(non_snake_case)]
            pub mod $name;
            pub use $name::$name;
        )*
    };

    ($($name:ident,)*) => {
        $(
            #[allow(non_snake_case)]
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
