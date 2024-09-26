pub mod builtin;
pub mod call;
pub use builtin::*;

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
