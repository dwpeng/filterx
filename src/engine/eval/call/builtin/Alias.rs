use super::*;

#[allow(non_snake_case)]
pub fn Alias<'a>(vm: &'a mut Vm, args: &Vec<ast::Expr>) -> FilterxResult<value::Value> {
    let v = alias(vm, args)?;
    assert!(v.is_column());

    match v {
        value::Value::Column(mut c) => {
            c.force = true;
            Ok(value::Value::Column(c))
        }
        _ => unreachable!(),
    }
}
