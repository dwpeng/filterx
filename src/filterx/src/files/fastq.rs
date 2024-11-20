use crate::args::{FastqCommand, ShareArgs};

use filterx_core::{util, writer::FilterxWriter, FilterxResult};
use filterx_engine::vm::Vm;
use filterx_source::{FastqSource, Source, SourceType};

use std::io::Write;
pub fn filterx_fastq(cmd: FastqCommand) -> FilterxResult<()> {
    let FastqCommand {
        share_args:
            ShareArgs {
                input: path,
                expr,
                output,
                table: _,
                output_type,
            },
        chunk: long,
        no_comment,
        no_quality,
        phred,
        limit,
        detect_size,
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
    let mut source = FastqSource::new(
        path.as_str(),
        !no_comment.unwrap(),
        !no_quality.unwrap(),
        phred.unwrap(),
        detect_size.unwrap(),
    )?;
    let mut writer = FilterxWriter::new(output.clone(), None, output_type)?;
    if expr.is_empty() {
        while let Some(record) = &mut source.fastq.parse_next()? {
            writeln!(writer, "{}", record.format())?;
        }
        return Ok(());
    }
    let chunk_size = long.unwrap();
    let mut vm = Vm::from_source(Source::new(source.into(), SourceType::Fastq), writer);
    vm.status.set_chunk_size(chunk_size);
    vm.source_mut().set_init_column_names(&names);
    'stop_parse: loop {
        let left = vm.next_batch()?;
        if left.is_none() {
            break 'stop_parse;
        }
        vm.eval_once(&expr)?;
        vm.finish()?;
        if !vm.status.printed {
            let df = vm.into_df()?;
            let writer = &mut vm.writer;
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
