use polars::prelude::IntoLazy;

use crate::cmd::args::*;
use crate::FilterxResult;
use std::io::Write;

use crate::engine::vm::{Vm, VmSourceType};
use crate::source::{DataframeSource, FastaSource, TableLike};

use crate::util;

pub fn filter_fasta(cmd: FastaCommand) -> FilterxResult<()> {
    let FastaCommand {
        share_args:
            ShareArgs {
                input: path,
                expr,
                output,
                table: _,
            },
        long,
    } = cmd;
    let mut source = FastaSource::new(path.as_str())?;
    let expr = expr.unwrap_or("".into());
    let mut output = util::create_buffer_writer(output)?;
    if expr.is_empty() {
        while let Some(record) = &mut source.fasta.parse_next()? {
            writeln!(output, "{}", record)?;
        }
        return Ok(());
    }
    let mut chunk_size = 1000;
    if long.is_some() {
        chunk_size = long.unwrap();
    }
    loop {
        let df = source.into_dataframe(chunk_size)?;
        if df.is_none() {
            break;
        }
        let df = df.unwrap();
        let mut vm = Vm::from_dataframe(DataframeSource::new(df.lazy()));
        vm.source_type = VmSourceType::Fasta;
        vm.eval_once(&expr)?;
        vm.finish()?;
        if !vm.status.printed {
            let df = vm.source.into_dataframe().unwrap().into_df()?;
            let cols = df.get_columns();
            let rows = df.height();
            for i in 0..rows {
                for col in cols {
                    match col.name().as_str() {
                        "seq" => {
                            let seq = col.get(i).unwrap();
                            let seq = seq.get_str().unwrap_or("");
                            write!(output, "{}\n", seq)?;
                        }
                        "name" => {
                            let name = col.get(i).unwrap();
                            let name = name.get_str().unwrap_or("");
                            write!(output, ">{}\n", name)?;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    Ok(())
}
