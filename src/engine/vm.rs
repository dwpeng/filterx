use polars::frame::DataFrame;
use polars::lazy::frame::LazyFrame;
use polars::prelude::*;

use super::eval::Eval;
use crate::FilterxResult;

pub struct Vm {
    /// register some columns name
    pub columns: Vec<String>,
    /// new_columns
    pub new_columns: Vec<String>,
    /// lazy LazyFrame
    pub lazy: LazyFrame,
    /// eval_expr
    pub eval_expr: String,
    /// finished dataframe
    pub df: Option<DataFrame>,
    /// apply_lazy
    pub apply_lazy: bool,
}

impl Vm {
    pub fn new(lazy: LazyFrame) -> Self {
        Self {
            columns: Vec::new(),
            new_columns: Vec::new(),
            lazy,
            eval_expr: String::new(),
            df: None,
            apply_lazy: true,
        }
    }

    fn ast(&self, s: &str) -> FilterxResult<rustpython_parser::ast::Mod> {
        if s.contains("=")
            && !s.contains("==")
            && !s.contains("!=")
            && !s.contains(">=")
            && !s.contains("<=")
        {
            let expr = rustpython_parser::parse(s, rustpython_parser::Mode::Interactive, "")?;
            return Ok(expr);
        } else {
            let expr = rustpython_parser::parse(s, rustpython_parser::Mode::Expression, "")?;
            return Ok(expr);
        }
    }

    pub fn eval(&mut self, expr: &str) -> FilterxResult<()> {
        // split the expr by ;
        if expr.is_empty() {
            return Ok(());
        }
        let exprs: Vec<&str> = expr.split(";").collect();
        for expr in exprs {
            self.eval_expr = expr.to_string();
            let expr = self.ast(expr)?;
            if expr.is_expression() {
                let expr = expr.as_expression().unwrap();
                expr.eval(self)?;
            } else if expr.is_interactive() {
                let expr = expr.as_interactive().unwrap();
                expr.eval(self)?;
            }
        }
        Ok(())
    }

    pub fn finish(&mut self) -> FilterxResult<()> {
        let df = self.lazy.clone().collect()?;
        self.df = Some(df);
        self.lazy = self.df.clone().unwrap().lazy();
        Ok(())
    }

    pub fn get_df(&self) -> Option<&DataFrame> {
        self.df.as_ref()
    }

    pub fn get_df_mut(&mut self) -> Option<&mut DataFrame> {
        self.df.as_mut()
    }

    pub fn get_lazy(&self) -> &LazyFrame {
        &self.lazy
    }
}

#[allow(unused_imports)]
mod test {
    use super::*;
    use crate::util;

    #[test]
    fn test_vm() {
        let mut vm = Vm::new(util::mock_lazy_df());
        let expr = "alias(c) = a + b";
        let ret = vm.eval(expr);
        println!("{:?}", ret);
        let ret = vm.finish();
        println!("{:?}", ret);
        let df = vm.get_df();
        println!("{:?}", df);
        assert!(df.is_some());
    }
}
