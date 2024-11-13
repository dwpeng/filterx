use super::super::ast;
use crate::eval::Eval;
use crate::vm::Vm;
use filterx_core::{value, FilterxResult};
use value::Value;

impl<'a> Eval<'a> for ast::ExprConstant {
    type Output = value::Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        let r = match &self.value {
            ast::Constant::Int(i) => {
                let v: Value = i.clone().into();
                v
            }
            ast::Constant::Float(f) => value::Value::Float(f.clone()),
            ast::Constant::Str(s) => value::Value::Str(s.clone()),
            ast::Constant::Bool(b) => value::Value::Bool(*b),
            ast::Constant::None => value::Value::Null,
            _ => {
                let h = &mut vm.hint;
                h.white("Only ")
                    .cyan("int")
                    .white(", ")
                    .cyan("float")
                    .white(", ")
                    .cyan("str")
                    .white(" are supported in expression.");
                h.print_and_exit();
            }
        };
        Ok(r)
    }
}

impl<'a> Eval<'a> for ast::ExprTuple {
    type Output = value::Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        let mut r = Vec::new();
        for e in &self.elts {
            match e {
                ast::Expr::Constant(c) => {
                    let v = c.eval(vm)?;
                    r.push(v);
                }
                ast::Expr::Call(c) => {
                    let v = c.eval(vm)?;
                    r.push(v);
                }
                _ => {
                    let h = &mut vm.hint;
                    h.white("Only ")
                        .cyan("int")
                        .white(", ")
                        .cyan("float")
                        .white(", ")
                        .cyan("str")
                        .white(" are supported in expression.");
                    h.print_and_exit();
                }
            }
        }
        Ok(value::Value::List(r))
    }
}

impl<'a> Eval<'a> for ast::ExprName {
    type Output = value::Value;
    fn eval(&self, vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        match self.ctx {
            ast::ExprContext::Del => {
                let h = &mut vm.hint;
                h.white("Can't use ").cyan("del").white(" on name.");
                h.print_and_exit();
            }
            _ => {}
        };
        let id = self.id.as_str();
        let name = value::Name {
            name: id.to_string(),
            ctx: match self.ctx {
                ast::ExprContext::Load => value::NameContext::Load,
                ast::ExprContext::Store => value::NameContext::Store,
                _ => unreachable!(),
            },
        };

        // impl keywords
        let keywords = match name.name.as_str() {
            _ => Value::None,
        };

        match keywords {
            Value::None => {}
            _ => return Ok(keywords),
        }

        Ok(value::Value::Name(name))
    }
}

impl<'a> Eval<'a> for ast::ExprAttribute {
    type Output = value::Value;
    fn eval(&self, _vm: &'a mut Vm) -> FilterxResult<Self::Output> {
        // TODO implement attribute
        Ok(value::Value::None)
    }
}
