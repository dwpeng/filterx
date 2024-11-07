use polars::frame::DataFrame;
use polars::prelude::IntoLazy;

use crate::cmd::args::*;
use crate::FilterxResult;
use std::io::Write;

use crate::engine::vm::{Vm, VmSourceType};
use crate::source::{FastaSource, Source, TableLike};

use crate::util;

pub fn filterx_fasta(cmd: FastaCommand) -> FilterxResult<()> {
    let FastaCommand {
        share_args:
            ShareArgs {
                input: path,
                expr,
                output,
                table: _,
            },
        chunk: long,
        no_comment,
        limit,
    } = cmd;

    let _limit = match limit {
        Some(l) => {
            if l == 0 {
                None
            } else {
                Some(l)
            }
        }
        None => None,
    };

    let names = match no_comment {
        Some(true) => vec!["name", "seq"],
        _ => vec!["name", "seq", "comm"],
    };

    let names = names.iter().map(|x| x.to_string()).collect();

    let expr = util::merge_expr(expr);
    let mut source = FastaSource::new(path.as_str(), !no_comment.unwrap())?;
    let output = util::create_buffer_writer(output)?;
    let mut output = Box::new(output);
    if expr.is_empty() {
        let mut buffer: Vec<u8> = Vec::new();
        while let Some(record) = &mut source.fasta.parse_next()? {
            writeln!(output, "{}", record.format(&mut buffer))?;
        }
        return Ok(());
    }
    let mut chunk_size = long.unwrap();
    let mut vm = Vm::from_dataframe(Source::new(DataFrame::empty().lazy()));
    vm.set_scope(VmSourceType::Fasta);
    vm.set_writer(output);
    'stop_parse: loop {
        chunk_size = usize::min(chunk_size, vm.status.limit - vm.status.cosumer_rows);
        let df = source.into_dataframe(chunk_size)?;
        if df.is_none() {
            break;
        }
        let mut dataframe_source = Source::new(df.unwrap().lazy());
        dataframe_source.set_init_column_names(&names);
        vm.source = dataframe_source;
        vm.next_batch()?;
        vm.eval_once(&expr)?;
        vm.finish()?;
        if !vm.status.printed {
            let writer = vm.writer.as_mut().unwrap();
            let df = vm.source.into_df()?;
            let cols = df.get_columns();
            let rows = df.height();
            for i in 0..rows {
                // TODO: handle the case where the column is not found
                // TODO: handle the case where the column's rank is not right
                for col in cols.iter() {
                    match col.name().as_str() {
                        "seq" => {
                            let seq = col.get(i).unwrap();
                            let seq = seq.get_str().unwrap_or("");
                            write!(writer, "\n{}\n", seq)?;
                        }
                        "comm" => {
                            let comment = col.get(i).unwrap();
                            let comment = comment.get_str().unwrap_or("");
                            write!(writer, " {}", comment)?;
                        }
                        "name" => {
                            let name = col.get(i).unwrap();
                            let name = name.get_str().unwrap_or("");
                            write!(writer, ">{}", name)?;
                        }
                        _ => {
                            break;
                        }
                    }
                }
                vm.status.cosumer_rows += 1;
                if vm.status.cosumer_rows >= vm.status.limit {
                    break 'stop_parse;
                }
            }
        }
    }
    Ok(())
}
