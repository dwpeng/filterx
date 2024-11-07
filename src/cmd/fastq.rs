use polars::frame::DataFrame;
use polars::prelude::IntoLazy;

use crate::cmd::args::*;
use crate::FilterxResult;
use std::io::Write;

use crate::engine::vm::{Vm, VmSourceType};
use crate::source::{FastqSource, Source, TableLike};

use crate::util;

pub fn filterx_fastq(cmd: FastqCommand) -> FilterxResult<()> {
    let FastqCommand {
        share_args:
            ShareArgs {
                input: path,
                expr,
                output,
                table: _,
            },
        chunk: long,
        no_comment,
        no_quality,
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

    let mut names = vec!["name", "seq", "comm", "qual"];

    match no_comment {
        Some(true) => {
            let index = names.iter().position(|x| x == &"comm").unwrap();
            names.remove(index);
        }
        _ => {}
    }

    match no_quality {
        Some(true) => {
            let index = names.iter().position(|x| x == &"qual").unwrap();
            names.remove(index);
        }
        _ => {}
    }

    let names = names.iter().map(|x| x.to_string()).collect::<Vec<String>>();
    let expr = util::merge_expr(expr);
    let mut source = FastqSource::new(path.as_str(), !no_comment.unwrap(), !no_quality.unwrap())?;
    let output = util::create_buffer_writer(output)?;
    let mut output = Box::new(output);
    if expr.is_empty() {
        let mut buffer: Vec<u8> = Vec::new();
        while let Some(record) = &mut source.fastq.parse_next()? {
            writeln!(output, "{}", record.format(&mut buffer))?;
        }
        return Ok(());
    }
    let mut chunk_size = long.unwrap();
    let mut vm = Vm::from_dataframe(Source::new(DataFrame::empty().lazy()));
    vm.set_scope(VmSourceType::Fastq);
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
                for col in cols.iter() {
                    match col.name().as_str() {
                        "name" => {
                            let name = col.get(i).unwrap();
                            let name = name.get_str().unwrap_or("");
                            write!(writer, "@{}", name)?;
                        }
                        "comm" => {
                            let comm = col.get(i).unwrap();
                            let comm = comm.get_str().unwrap_or("");
                            write!(writer, " {}", comm)?;
                        }
                        "seq" => {
                            let seq = col.get(i).unwrap();
                            let seq = seq.get_str().unwrap_or("");
                            write!(writer, "\n{}\n", seq)?;
                        }
                        "qual" => {
                            let qual = col.get(i).unwrap();
                            let qual = qual.get_str().unwrap_or("");
                            write!(writer, "+\n{}\n", qual)?;
                        }
                        _ => {
                            break;
                        }
                    }
                }
            }
            vm.status.cosumer_rows += 1;
            if vm.status.cosumer_rows >= vm.status.limit {
                break 'stop_parse;
            }
        }
    }
    Ok(())
}
