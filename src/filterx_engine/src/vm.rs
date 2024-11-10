use std::collections::HashMap;

use filterx_core::{FilterxError, FilterxResult, Hint};

use filterx_source::Source;

use super::eval_vm::Eval;

use std::io::BufWriter;
use std::io::Write;

pub enum VmMode {
    Interactive,
    Expression,
}

#[derive(Debug)]
pub struct VmStatus {
    pub apply_lazy: bool,
    pub stop: bool,
    pub count: usize,
    pub limit_rows: usize,
    pub offset: usize,
    pub printed: bool,
    pub consume_rows: usize,
}

impl VmStatus {
    pub fn new() -> Self {
        Self {
            apply_lazy: true,
            stop: false,
            count: 0,
            limit_rows: usize::MAX,
            offset: 0,
            printed: false,
            consume_rows: 0,
        }
    }
}

impl VmStatus {
    pub fn update_apply_lazy(&mut self, apply_lazy: bool) {
        self.apply_lazy = apply_lazy;
    }
}

impl Default for VmStatus {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, clap::ValueEnum, PartialEq)]
pub enum VmSourceType {
    Csv,
    Fasta,
    Fastq,
    Vcf,
    Sam,
    Gxf,
}

impl Into<&str> for VmSourceType {
    fn into(self) -> &'static str {
        match self {
            VmSourceType::Csv => "csv",
            VmSourceType::Fasta => "fasta",
            VmSourceType::Fastq => "fastq",
            VmSourceType::Vcf => "vcf",
            VmSourceType::Sam => "sam",
            VmSourceType::Gxf => "gxf",
        }
    }
}

impl From<&str> for VmSourceType {
    fn from(s: &str) -> Self {
        match s {
            "csv" => VmSourceType::Csv,
            "fasta" => VmSourceType::Fasta,
            "fastq" => VmSourceType::Fastq,
            "vcf" => VmSourceType::Vcf,
            "sam" => VmSourceType::Sam,
            "gxf" => VmSourceType::Gxf,
            _ => panic!("Invalid source type"),
        }
    }
}

pub struct Vm {
    /// eval_expr
    pub eval_expr: String,
    pub parse_cache: HashMap<String, rustpython_parser::ast::Mod>,
    /// mode
    pub mode: VmMode,
    /// source
    pub source: Source,
    pub status: VmStatus,
    pub source_type: VmSourceType,
    pub writer: Option<Box<BufWriter<Box<dyn Write>>>>,
    pub expr_cache: HashMap<String, (String, Vec<polars::prelude::Expr>)>,
    pub hint: Hint,
}

impl Vm {
    pub fn from_dataframe(dataframe: Source) -> Self {
        Self {
            eval_expr: String::new(),
            parse_cache: HashMap::new(),
            mode: VmMode::Expression,
            source: dataframe,
            status: VmStatus::default(),
            source_type: VmSourceType::Csv,
            writer: None,
            expr_cache: HashMap::new(),
            hint: Hint::new(),
        }
    }

    pub fn set_mode(&mut self, mode: VmMode) {
        self.mode = mode;
    }

    pub fn set_scope(&mut self, scope: VmSourceType) {
        self.source_type = scope;
    }

    pub fn set_writer(&mut self, writer: Box<BufWriter<Box<dyn Write>>>) {
        self.writer = Some(writer);
    }

    pub fn ast(&self, s: &str) -> FilterxResult<rustpython_parser::ast::Mod> {
        let s = s.trim();
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

    pub fn valid_exprs(&mut self, expr: &str) -> FilterxResult<bool> {
        let exprs: Vec<&str> = expr.split(";").collect();
        for expr in exprs.clone() {
            if expr.is_empty() {
                continue;
            }
            let a = self.ast(expr);
            if a.is_err() {
                let h = &mut self.hint;
                let err = a.err().unwrap();
                match err {
                    FilterxError::ParseError(e) => {
                        let pos = e.offset;
                        h.white("expr: ")
                            .cyan(expr)
                            .white(" gets a parse error ")
                            .next_line()
                            .white(&(" ".repeat(pos.to_usize() + 5)))
                            .red(&format!("^{}", e.error.to_string()))
                            .print_and_exit();
                    }
                    _ => {
                        h.white("expr: ")
                            .cyan(expr)
                            .white(" gets a parse error ")
                            .red(&format!("{}", err))
                            .print_and_exit();
                    }
                }
            }
        }
        Ok(true)
    }

    pub fn eval_once(&mut self, expr: &str) -> FilterxResult<()> {
        // split the expr by ;
        if expr.is_empty() {
            return Ok(());
        }
        // check ast process
        let exprs: Vec<&str> = expr.split(";").collect();

        for expr in exprs {
            if expr.is_empty() {
                continue;
            }
            self.eval_expr = expr.to_string();
            let eval_expr;
            if self.parse_cache.contains_key(expr) {
                eval_expr = self.parse_cache.get(expr).unwrap().clone();
            } else {
                eval_expr = self.ast(expr)?;
                self.parse_cache.insert(expr.to_string(), eval_expr.clone());
            }
            if eval_expr.is_expression() {
                let expr = eval_expr.as_expression().unwrap();
                expr.eval(self)?;
            } else if eval_expr.is_interactive() {
                let expr = eval_expr.as_interactive().unwrap();
                expr.eval(self)?;
            }
            // if self.status.stop {
            //     std::process::exit(0);
            // }
            // if self.status.printed {
            //     return Ok(());
            // }
        }
        Ok(())
    }

    pub fn next_batch(&mut self) -> FilterxResult<()> {
        self.status.printed = false;
        Ok(())
    }

    pub fn finish(&mut self) -> FilterxResult<()> {
        self.source.finish()
    }
}

#[allow(unused_imports)]
mod test {
    use super::*;
    use filterx_core::util;

    #[test]
    fn test_vm() {
        let frame = Source::new(util::mock_lazy_df());
        let mut vm = Vm::from_dataframe(frame);
        let expr = "alias(c) = a + b";
        let ret = vm.eval_once(expr);
        println!("{:?}", ret);
        let ret = vm.finish();
        println!("{:?}", ret);
    }
}
