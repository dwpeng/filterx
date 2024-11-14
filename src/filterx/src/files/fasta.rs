use crate::args::{FastaCommand, ShareArgs};
use std::io::Write;

use filterx_engine::vm::Vm;
use filterx_source::{FastaSource, Source, SourceType};

use filterx_core::{util, FilterxResult};

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
        while let Some(record) = &mut source.fasta.parse_next()? {
            writeln!(output, "{}", record.format())?;
        }
        return Ok(());
    }
    let chunk_size = long.unwrap();
    let mut vm = Vm::from_source(Source::new(source.into(), SourceType::Fasta));
    vm.source.df_source_mut().set_init_column_names(&names);
    vm.set_writer(output);
    vm.status.set_chunk_size(chunk_size);
    'stop_parse: loop {
        let left = vm.next_batch()?;
        if left.is_none() {
            break 'stop_parse;
        }
        vm.eval_once(&expr)?;
        vm.finish()?;
        if !vm.status.printed {
            let df = vm.into_df()?;
            let writer = vm.writer.as_mut().unwrap();
            let cols = df.get_columns();
            let seq_col = cols.iter().position(|x| x.name() == "seq");
            let name_col = cols.iter().position(|x| x.name() == "name");
            if seq_col.is_none() {
                let h = &mut vm.hint;
                h.white("Lost ")
                    .cyan("'seq'")
                    .white(" column.")
                    .print_and_exit();
            }
            if name_col.is_none() {
                let h = &mut vm.hint;
                h.white("Lost ")
                    .cyan("'name'")
                    .white(" column.")
                    .print_and_exit();
            }
            let cols = df.get_columns();
            let seq_col = seq_col.unwrap();
            let name_col = name_col.unwrap();

            let comm_col = match no_comment {
                Some(true) => None,
                _ => cols.iter().position(|x| x.name() == "comm"),
            };

            let valid_cols;
            if comm_col.is_some() {
                valid_cols = vec![name_col, comm_col.unwrap(), seq_col]
            } else {
                valid_cols = vec![name_col, seq_col]
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
            }
        }
    }
    Ok(())
}
