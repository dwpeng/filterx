use std::str::FromStr;

use polars::prelude::*;

use crate::source::{DataframeSource, FastaSource, FastqSource, Source};

use super::eval::Eval;
use crate::FilterxResult;

pub enum VmMode {
    Interactive,
    Expression,
}

#[derive(Debug, Default, Clone)]
pub struct Col {
    pub name: String,
    pub new: bool,
    pub data_type: DataType,
    pub index: usize,
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
    columns: Vec<Col>,
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
            selected_columns: Vec::new(),
        }
    }
}

impl VmStatus {
    pub fn inject_columns_by_df(&mut self, df: LazyFrame) -> FilterxResult<()> {
        let df = df.lazy().with_streaming(true).fetch(1)?;
        let dtypes = df.dtypes();
        for (i, col) in df.get_columns().iter().enumerate() {
            let c = Col {
                name: col.name().to_string(),
                new: false,
                data_type: dtypes[i].clone(),
                index: i,
            };
            self.columns.push(c);
        }
        self.selected_columns = self.columns.clone();
        Ok(())
    }

    pub fn inject_columns_by_default(&mut self, cols: Vec<Col>) {
        self.columns = cols;
        self.selected_columns = self.columns.clone();
    }

    pub fn add_new_column(&mut self, name: &str, t: DataType) {
        let col = Col {
            name: name.to_string(),
            new: true,
            data_type: t,
            index: self.columns.len(),
        };
        self.selected_columns.push(col);
    }

    pub fn drop_column(&mut self, name: &str) {
        self.selected_columns.retain(|x| x.name != name);
    }

    pub fn reset_selected_columns(&mut self) {
        if self.selected_columns.len() < self.columns.len() {
            // resize
            self.selected_columns
                .resize(self.columns.len(), Col::default());
        }
        // set selected_columns to columns
        for (i, col) in self.columns.iter().enumerate() {
            let c = &mut self.selected_columns[i];
            c.name.clear();
            c.name.push_str(&col.name);
            c.data_type = col.data_type.clone();
            c.index = i;
        }
    }

    pub fn select(&mut self, col: Vec<String>) {
        let selected_columns = self.selected_columns.clone();
        self.selected_columns = selected_columns
            .into_iter()
            .filter(|x| col.contains(&x.name))
            .collect();
    }

    pub fn is_column_exist(&self, name: &str) -> bool {
        for col in &self.selected_columns {
            if col.name == name {
                return true;
            }
        }
        false
    }

    pub fn update_apply_lazy(&mut self, apply_lazy: bool) {
        self.apply_lazy = apply_lazy;
    }
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum VmSourceType {
    Csv,
    Fasta,
    Fastq,
    Vcf,
    Sam,
    Gff,
    Gtf,
}

impl FromStr for VmSourceType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "csv" => Ok(VmSourceType::Csv),
            "fasta" => Ok(VmSourceType::Fasta),
            "fastq" => Ok(VmSourceType::Fastq),
            "vcf" => Ok(VmSourceType::Vcf),
            "sam" => Ok(VmSourceType::Sam),
            "gff" => Ok(VmSourceType::Gff),
            "gtf" => Ok(VmSourceType::Gtf),
            _ => Err(()),
        }
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
    pub source_type: VmSourceType,
}

impl Vm {
    pub fn from_dataframe(dataframe: DataframeSource) -> Self {
        Self {
            eval_expr: String::new(),
            mode: VmMode::Expression,
            source: Source::new_dataframe(dataframe),
            status: VmStatus::default(),
            source_type: VmSourceType::Csv,
        }
    }

    pub fn from_fasta(fasta: FastaSource) -> Self {
        Self {
            eval_expr: String::new(),
            mode: VmMode::Expression,
            source: Source::new_fasta(fasta),
            status: VmStatus::default(),
            source_type: VmSourceType::Fasta,
        }
    }

    pub fn from_fastq(fastq: FastqSource) -> Self {
        Self {
            eval_expr: String::new(),
            mode: VmMode::Expression,
            source: Source::new_fastq(fastq),
            status: VmStatus::default(),
            source_type: VmSourceType::Fastq,
        }
    }

    pub fn set_mode(&mut self, mode: VmMode) {
        self.mode = mode;
    }

    pub fn set_scope(&mut self, scope: VmSourceType) {
        self.source_type = scope;
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

    pub fn exprs_to_ast(
        &self,
        exprs: Vec<&str>,
    ) -> FilterxResult<Vec<rustpython_parser::ast::Mod>> {
        let mut asts = Vec::new();
        for expr in exprs {
            let ast = self.ast(expr)?;
            asts.push(ast);
        }
        Ok(asts)
    }

    pub fn eval_once(&mut self, expr: &str) -> FilterxResult<()> {
        // split the expr by ;
        if expr.is_empty() {
            return Ok(());
        }
        let exprs: Vec<&str> = expr.split(";").collect();
        for expr in exprs {
            if expr.is_empty() {
                continue;
            }
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

    pub fn eval_many(&mut self, exprs: Vec<&str>) -> FilterxResult<()> {
        let asts = self.exprs_to_ast(exprs)?;
        while self.status.stop == false {
            for ast in &asts {
                if ast.is_expression() {
                    let expr = ast.as_expression().unwrap();
                    expr.eval(self)?;
                } else if ast.is_interactive() {
                    let expr = ast.as_interactive().unwrap();
                    expr.eval(self)?;
                }
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
        let ret = vm.eval_once(expr);
        println!("{:?}", ret);
        let ret = vm.finish();
        println!("{:?}", ret);
    }
}
