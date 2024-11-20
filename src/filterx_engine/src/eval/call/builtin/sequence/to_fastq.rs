use super::super::*;
use std::io::Write;

fn print_fastq(vm: &mut Vm) -> FilterxResult<value::Value> {
    vm.status.printed = true;
    let name_index = vm.source_mut().ret_column_names.iter().position(|x| x == "name");
    let seq_index = vm.source_mut().ret_column_names.iter().position(|x| x == "seq");
    let qual_index = vm.source_mut().ret_column_names.iter().position(|x| x == "qual");
    if name_index.is_none() {
        let h = &mut vm.hint;
        h.white("Lost ")
            .cyan("'name'")
            .white(" column.")
            .print_and_exit();
    }
    if seq_index.is_none() {
        let h = &mut vm.hint;
        h.white("Lost ")
            .cyan("'seq'")
            .white(" column.")
            .print_and_exit();
    }
    let name_index = name_index.unwrap();
    let seq_index = seq_index.unwrap();
    let df = vm.source_mut().lazy().collect()?;
    let columns = df.get_columns();
    let name_col = &columns[name_index];
    let seq_col = &columns[seq_index];
    if name_col.len() != seq_col.len() {
        let h = &mut vm.hint;
        h.white("Length of ")
            .cyan("'name'")
            .white(" and ")
            .cyan("'seq'")
            .white(" columns are different.")
            .print_and_exit();
    }
    let writer = &mut vm.writer;
    if qual_index.is_some() {
        let qual_index = qual_index.unwrap();
        let qual_col = &columns[qual_index];
        for i in 0..name_col.len() {
            let name = name_col.get(i).unwrap();
            let seq = seq_col.get(i).unwrap();
            let q = qual_col.get(i).unwrap();
            let _ = writeln!(writer, "@{}", name.get_str().unwrap_or("name"))?;
            let _ = writeln!(writer, "{}", seq.get_str().unwrap_or("seq"))?;
            let _ = writeln!(writer, "+")?;
            let _ = writeln!(writer, "{}", q.get_str().unwrap_or("qual"))?;
            vm.status.consume_rows += 1;
            if vm.status.consume_rows >= vm.status.limit_rows {
                vm.status.stop = true;
                break;
            }
        }
    } else {
        let mut qual_buffer = String::with_capacity(512);
        for i in 0..name_col.len() {
            let name = name_col.get(i).unwrap();
            let seq = seq_col.get(i).unwrap();
            let seq = seq.get_str().unwrap_or("seq");
            let _ = writeln!(writer, "@{}", name.get_str().unwrap_or("name"))?;
            let _ = writeln!(writer, "{}", seq)?;
            let _ = writeln!(writer, "+")?;
            if qual_buffer.len() != seq.len() {
                if qual_buffer.len() > seq.len() {
                    qual_buffer.truncate(seq.len());
                } else {
                    qual_buffer.push_str(&"?".repeat(seq.len() - qual_buffer.len()))
                }
            }
            let _ = writeln!(writer, "{}", &qual_buffer)?;
            vm.status.consume_rows += 1;
            if vm.status.consume_rows >= vm.status.limit_rows {
                vm.status.stop = true;
                break;
            }
        }
    }

    Ok(value::Value::None)
}

pub fn to_fastq(vm: &mut Vm) -> FilterxResult<value::Value> {
    if vm.source_type() == SourceType::Fasta || vm.source_type() == SourceType::Fastq {
        return print_fastq(vm);
    }
    let h = &mut vm.hint;
    h.white("Only ")
        .cyan("fastq, fasta ")
        .white("formats are supported for now.")
        .print_and_exit();
}
