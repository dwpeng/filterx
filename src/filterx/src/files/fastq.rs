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
                sql,
            },
        chunk: long,
        no_comment,
        no_quality,
        phred,
        limit,
        detect_size,
    } = cmd;

    // 0 means no limit, keep the same semantics as the fasta/csv commands
    let limit = match limit {
        Some(0) => None,
        other => other,
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
    let mut chunk_size = long.unwrap();
    if let Some(limit) = limit {
        chunk_size = chunk_size.min(limit);
    }
    let mut count = 0;
    let mut writer = FilterxWriter::new(output.clone(), None, output_type)?;
    if expr.is_empty() && sql.is_none() {
        while let Some(record) = &mut source.fastq.parse_next()? {
            if let Some(limit) = limit {
                if count >= limit {
                    break;
                }
            }
            writeln!(writer, "{}", record.format())?;
            count += 1;
        }
        return Ok(());
    }

    let mut vm = Vm::from_source(Source::new(source.into(), SourceType::Fastq), writer);
    vm.status.set_chunk_size(chunk_size);
    vm.status.set_limit_rows(limit.unwrap_or(usize::MAX));
    vm.source_mut().set_init_column_names(&names);
    'stop_parse: loop {
        let left = vm.next_batch()?;
        if left.is_none() {
            break 'stop_parse;
        }
        vm.eval_once(&expr, sql.clone())?;
        if !vm.status.printed {
            let df = vm.into_df()?;
            let writer = &mut vm.writer;
            let cols = df.get_columns();
            let no_qual = no_quality.unwrap_or(false);
            let no_comm = no_comment.unwrap_or(false);

            let name_col = cols.iter().position(|x| x.name() == "name");
            let seq_col = cols.iter().position(|x| x.name() == "seq");

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

            // --no-qual: the parser never produces a qual column and the
            // no-expression fast path emits FASTA, so do the same here.
            let qual_col = if no_qual {
                None
            } else {
                cols.iter().position(|x| x.name() == "qual")
            };
            if qual_col.is_none() && !no_qual {
                let h = &mut vm.hint;
                h.white("Lost ")
                    .cyan("'qual'")
                    .white(" column.")
                    .print_and_exit();
            }

            let comm_col = if no_comm {
                None
            } else {
                cols.iter().position(|x| x.name() == "comm")
            };

            // keep the semantic order: name, comm, seq, qual
            let mut valid_cols = vec![name_col.unwrap()];
            if let Some(comm_col) = comm_col {
                valid_cols.push(comm_col);
            }
            valid_cols.push(seq_col.unwrap());
            if let Some(qual_col) = qual_col {
                valid_cols.push(qual_col);
            }

            let rows = df.height();
            for i in 0..rows {
                if vm.status.consume_rows >= vm.status.limit_rows {
                    break 'stop_parse;
                }
                vm.status.consume_rows += 1;
                for col_index in &valid_cols {
                    let col = &cols[*col_index];
                    let value = col.get(i).unwrap();
                    let value = value.get_str().unwrap_or("");
                    match col.name().as_str() {
                        "name" => {
                            if no_qual {
                                write!(writer, ">{}", value)?;
                            } else {
                                write!(writer, "@{}", value)?;
                            }
                        }
                        "comm" => {
                            write!(writer, " {}", value)?;
                        }
                        "seq" => {
                            write!(writer, "\n{}\n", value)?;
                        }
                        "qual" => {
                            write!(writer, "+\n{}\n", value)?;
                        }
                        _ => {
                            break;
                        }
                    }
                }
            }
            writer.flush()?;
            if let Some(limit) = limit {
                if vm.status.consume_rows >= limit {
                    break 'stop_parse;
                }
            }
        }
    }
    Ok(())
}
