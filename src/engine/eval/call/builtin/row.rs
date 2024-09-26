use super::*;

pub fn row<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    let mut r = Vec::new();
    for e in args {
        match e {
            ast::Expr::Constant(c) => {
                let v = c.eval(vm)?;
                r.push(v);
            }
            ast::Expr::Call(c) => {
                let v = c.eval(vm)?;
                r.push(v);
            }
            // do not support multi-dimensional slice
            // ast::Expr::Slice(s) => {
            //     let v = s.eval(vm)?;
            //     r.push(v);
            // }
            _ => {
                return Err(FilterxError::RuntimeError(
                    "Only support constant".to_string(),
                ))
            }
        }
    }
    Ok(value::Value::List(r))
}
