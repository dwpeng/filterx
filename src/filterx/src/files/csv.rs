use crate::args::{CsvCommand, ShareArgs};
use filterx_core::{util, writer::FilterxWriter, FilterxResult};
use filterx_engine::vm::Vm;
use filterx_source::{detect_columns, DataframeSource, Source, SourceType};

pub fn filterx_csv(cmd: CsvCommand) -> FilterxResult<()> {
    let CsvCommand {
        share_args:
            ShareArgs {
                input: path,
                expr,
                output,
                table,
                output_type,
                sql,
            },
        header,
        no_header,
        comment_prefix,
        separator,
        output_separator: _output_separator,
        skip,
        limit,
    } = cmd;
    let separator = match separator {
        Some(s) => Some(s),
        None => match util::detect_separator(path.as_str(), 20, skip, comment_prefix.clone())? {
            Some(s) => Some(s),
            None => {
                eprintln!("Cannot detect separator, parse as one column.");
                None
            }
        },
    };
    let output_separator;
    if _output_separator.is_none() {
        output_separator = separator.clone();
    } else {
        output_separator = _output_separator;
    }
    let mut output_header = Some(true);
    if no_header.is_some_and(|v| v == true) || header.is_some_and(|v| v == false) {
        output_header = Some(false);
    }
    let limit = match limit {
        Some(l) => {
            if l == 0 {
                None
            } else {
                Some(l)
            }
        }
        None => None,
    };
    let comment_prefix = comment_prefix.unwrap();
    let writer = FilterxWriter::new(output.clone(), None, output_type)?;
    let lazy_df = util::init_df(
        path.as_str(),
        header.unwrap(),
        &comment_prefix,
        separator.as_deref(),
        skip.unwrap(),
        limit,
        None,
        None,
        true,
    )?;
    let columns = detect_columns(lazy_df.clone())?;
    let mut s = DataframeSource::new(lazy_df.clone());
    s.set_has_header(header.unwrap());
    s.set_init_column_names(&columns);
    let mut vm = Vm::from_source(Source::new(s.into(), SourceType::Csv), writer);
    vm.source_mut().set_has_header(header.unwrap());
    let expr = util::merge_expr(expr);
    vm.eval_once(&expr, sql)?;
    if vm.status.printed {
        return Ok(());
    }
    let mut df = vm.into_df()?;

    if output.is_none() && table.unwrap_or(false) {
        println!("{}", df);
        return Ok(());
    }
    if vm.status.printed {
        return Ok(());
    }
    util::write_df(
        &mut df,
        &mut vm.writer,
        output_header.unwrap(),
        output_separator.as_deref(),
        None,
        None,
    )
}
