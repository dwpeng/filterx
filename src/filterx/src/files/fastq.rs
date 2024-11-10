use polars::frame::DataFrame;
use polars::prelude::IntoLazy;

use crate::args::{FastqCommand, ShareArgs};
use std::io::Write;

use filterx_engine::vm::{Vm, VmSourceType};
use filterx_source::{FastqSource, Source, TableLike};

use filterx_core::{util, FilterxResult};
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

    let mut names = vec!["name", "comm", "seq", "qual"];

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
        if vm.status.consume_rows >= vm.status.limit_rows {
            break;
        }
        chunk_size = usize::min(chunk_size, vm.status.limit_rows - vm.status.consume_rows);
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
            let name_col = cols.iter().position(|x| x.name() == "name");
            let seq_col = cols.iter().position(|x| x.name() == "seq");
            let qual_col = cols.iter().position(|x| x.name() == "qual");

            if name_col.is_none() {
                let h = &mut vm.hint;
                h.white("Lost ")
                    .cyan("'name'")
                    .white(" column.")
                    .print_and_exit();
            }

            if seq_col.is_none() {
                let h = &mut vm.hint;
                h.white("Lost ")
                    .cyan("'seq'")
                    .white(" column.")
                    .print_and_exit();
            }

            if qual_col.is_none() {
                let h = &mut vm.hint;
                h.white("Lost ")
                    .cyan("'qual'")
                    .white(" column.")
                    .print_and_exit();
            }

            let valid_cols;
            let comm_col = match no_comment {
                Some(true) => None,
                _ => cols.iter().position(|x| x.name() == "comm"),
            };

            if comm_col.is_some() {
                valid_cols = vec![
                    name_col.unwrap(),
                    comm_col.unwrap(),
                    seq_col.unwrap(),
                    qual_col.unwrap(),
                ]
            } else {
                valid_cols = vec![name_col.unwrap(), seq_col.unwrap(), qual_col.unwrap()]
            }

            let rows = df.height();
            for i in 0..rows {
                if vm.status.consume_rows >= vm.status.limit_rows {
                    break 'stop_parse;
                }
                vm.status.consume_rows += 1;
                for col_index in &valid_cols {
                    let col = &cols[*col_index];
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
                            let qual = qual.get_str().unwrap_or("space");
                            write!(writer, "+\n{}\n", qual)?;
                        }
                        _ => {
                            break;
                        }
                    }
                }
            }
            writer.flush()?;
        }
    }
    Ok(())
}