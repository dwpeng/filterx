use polars::prelude::*;

use crate::source::{DataframeSource, FastaSource, FastqSource, Source};

use super::eval::Eval;
use crate::FilterxResult;

pub enum VmMode {
    Interactive,
    Expression,
}

#[derive(Debug, Default)]
pub struct Col {
    pub name: String,
    pub new: bool,
}

#[derive(Debug, Default)]
pub struct VmStatus {
    pub apply_lazy: bool,
    pub skip: bool,
    pub stop: bool,
    pub print: bool,
    pub count: usize,
    pub limit: usize,
    pub offset: usize,
    pub columns: Vec<Col>,
    pub indexs: Vec<usize>,
    pub types: Vec<DataType>,
    pub selected_columns: Vec<Col>,
}

impl VmStatus {
    pub fn new() -> Self {
        Self {
            apply_lazy: true,
            skip: false,
            stop: false,
            print: false,
            count: 0,
            limit: 0,
            offset: 0,
            columns: Vec::new(),
            indexs: Vec::new(),
            types: Vec::new(),
            selected_columns: Vec::new(),
        }
    }
}

impl VmStatus {
    pub fn add_column(&mut self, name: &str, new: bool, t: DataType) {
        self.columns.push(Col {
            name: name.to_string(),
            new,
        });

        if new {
            self.selected_columns.push(Col {
                name: name.to_string(),
                new,
            });
            self.indexs.push(self.columns.len() - 1);
            self.types.push(t);
        }
    }

    pub fn is_column_exist(&self, name: &str) -> bool {
        for col in &self.columns {
            if col.name == name {
                return true;
            }
        }
        false
    }

    pub fn add_selected_column(&mut self, name: &str) {
        for (i, col) in self.columns.iter().enumerate() {
            if col.name == name {
                self.selected_columns.push(Col {
                    name: name.to_string(),
                    new: col.new,
                });
                self.indexs.push(i);
                self.types.push(DataType::Null);
            }
        }
    }

    pub fn update_apply_lazy(&mut self, apply_lazy: bool) {
        self.apply_lazy = apply_lazy;
    }
}

pub struct Vm {
    /// eval_expr
    pub eval_expr: String,
    /// mode
    pub mode: VmMode,
    /// source
    pub source: Source,
    pub status: VmStatus,
}

impl Vm {
    pub fn from_dataframe(dataframe: DataframeSource) -> Self {
        Self {
            eval_expr: String::new(),
            mode: VmMode::Expression,
            source: Source::new_dataframe(dataframe),
            status: VmStatus::default(),
        }
    }

    pub fn from_fasta(fasta: FastaSource) -> Self {
        Self {
            eval_expr: String::new(),
            mode: VmMode::Expression,
            source: Source::new_fasta(fasta),
            status: VmStatus::default(),
        }
    }

    pub fn from_fastq(fastq: FastqSource) -> Self {
        Self {
            eval_expr: String::new(),
            mode: VmMode::Expression,
            source: Source::new_fastq(fastq),
            status: VmStatus::default(),
        }
    }

    pub fn set_mode(&mut self, mode: VmMode) {
        self.mode = mode;
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
        self.source.finish()
    }
}

#[allow(unused_imports)]
mod test {
    use super::*;
    use crate::util;

    #[test]
    fn test_vm() {
        let frame = DataframeSource::new(util::mock_lazy_df());
        let mut vm = Vm::from_dataframe(frame);
        let expr = "alias(c) = a + b";
        let ret = vm.eval(expr);
        println!("{:?}", ret);
        let ret = vm.finish();
        println!("{:?}", ret);
    }
}
