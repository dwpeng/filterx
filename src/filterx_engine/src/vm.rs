use std::collections::HashMap;

use polars::prelude::*;

use filterx_core::{FilterxError, FilterxResult, Hint};
use filterx_source::source::SourceType;
use filterx_source::{
    DataframeSource, FastaRecordType, FastaSource, FastqSource, QualityType, Source, SourceInner,
};

use super::eval::Eval;

use std::io::BufWriter;
use std::io::Write;

#[derive(Debug, PartialEq)]
pub enum VmMode {
    Expression,
    Printable,
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
    pub chunk_size: usize,
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
            chunk_size: 10000,
        }
    }
}

impl VmStatus {
    pub fn update_apply_lazy(&mut self, apply_lazy: bool) {
        self.apply_lazy = apply_lazy;
    }

    pub fn set_limit_rows(&mut self, limit_rows: usize) {
        self.limit_rows = limit_rows;
    }
    pub fn set_chunk_size(&mut self, chunk_size: usize) {
        self.chunk_size = chunk_size;
    }
}

impl Default for VmStatus {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Vm {
    /// eval_expr
    pub eval_expr: String,
    pub print_expr: String,
    pub parse_cache: HashMap<String, rustpython_parser::ast::Mod>,
    /// mode
    pub mode: VmMode,
    /// source
    pub source: Source,
    pub status: VmStatus,
    pub writer: Option<Box<BufWriter<Box<dyn Write>>>>,
    pub expr_cache: HashMap<String, (String, Vec<polars::prelude::Expr>)>,
    pub hint: Hint,
}

impl Vm {
    pub fn mock(source_type: SourceType) -> Vm {
        let innser: SourceInner = match source_type {
            SourceType::Fasta => FastaSource::new("", false, FastaRecordType::Dna, 0)
                .unwrap()
                .into(),
            SourceType::Fastq => FastqSource::new("", false, false, QualityType::Phred33, 0)
                .unwrap()
                .into(),
            _ => DataframeSource::new(DataFrame::empty().lazy()).into(),
        };

        let vm = Vm {
            eval_expr: "".to_string(),
            print_expr: "".to_string(),
            parse_cache: HashMap::new(),
            mode: VmMode::Expression,
            source: Source::new(innser, source_type),
            status: VmStatus::default(),
            writer: None,
            expr_cache: HashMap::new(),
            hint: Hint::new(),
        };
        vm
    }

    pub fn from_source(source: Source) -> Self {
        Self {
            eval_expr: String::new(),
            print_expr: String::new(),
            parse_cache: HashMap::new(),
            mode: VmMode::Expression,
            source,
            status: VmStatus::default(),
            writer: None,
            expr_cache: HashMap::new(),
            hint: Hint::new(),
        }
    }

    pub fn set_print_expr(&mut self, print_expr: &str) {
        self.print_expr.clear();
        self.print_expr.push_str(print_expr);
    }

    pub fn set_mode(&mut self, mode: VmMode) {
        self.mode = mode;
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
            && !s.contains("print(")
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
            self.eval_expr.clear();
            self.eval_expr.push_str(expr);
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

    pub fn next_batch(&mut self) -> FilterxResult<Option<()>> {
        self.status.printed = false;
        match self.source_type() {
            SourceType::Fasta | SourceType::Fastq => {
                if self.status.stop {
                    return Ok(None);
                }
                let left = self.status.limit_rows - self.status.consume_rows;
                let left = left.min(self.status.chunk_size);
                if left > 0 {
                    match self.source.inner {
                        SourceInner::Fasta(ref mut fasta) => {
                            let count = fasta.into_dataframe(left)?;
                            if count < left || count == 0 {
                                self.status.stop = true;
                            }
                        }
                        SourceInner::Fastq(ref mut fastq) => {
                            let count = fastq.into_dataframe(left)?;
                            if count < left || count == 0 {
                                self.status.stop = true;
                            }
                        }
                        _ => {
                            unreachable!();
                        }
                    }
                    return Ok(Some(()));
                }
                Ok(None)
            }
            _ => Ok(Some(())),
        }
    }

    pub fn source_mut(&mut self) -> &mut DataframeSource {
        self.source.df_source_mut()
    }

    pub fn source(&self) -> &DataframeSource {
        self.source.df_source()
    }

    pub fn into_df(&self) -> FilterxResult<DataFrame> {
        self.source.into_df()
    }

    pub fn source_type(&self) -> SourceType {
        self.source.source_type
    }

    pub fn finish(&mut self) -> FilterxResult<()> {
        let s = self.source.df_source_mut();
        s.finish()
    }
}

#[allow(unused_imports)]
mod test {
    use super::*;
    use filterx_core::util;
    use filterx_source::source::{SourceInner, SourceType};
    use filterx_source::DataframeSource;

    #[test]
    fn test_vm() {
        let frame = DataframeSource::new(util::mock_lazy_df());
        let source = Source::new(frame.into(), SourceType::Csv);
        let mut vm = Vm::from_source(source);
        let expr = "alias(c) = a + b";
        let ret = vm.eval_once(expr);
        println!("{:?}", ret);
        let ret = vm.finish();
        println!("{:?}", ret);
    }
}
