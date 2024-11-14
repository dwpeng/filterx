use std::borrow::Cow;

use super::super::*;

use filterx_source::FastaRecordType;
use polars::prelude::*;

fn compute_revcomp_dna(s: Column) -> PolarsResult<Option<Column>> {
    let ca = s.str()?;
    let ca = ca.apply_values(|s| {
        let s: String = s
            .chars()
            .rev()
            .map(|b| match b {
                'A' => 'T',
                'T' => 'A',
                'C' => 'G',
                'G' => 'C',
                'a' => 't',
                't' => 'a',
                'c' => 'g',
                'g' => 'c',
                _ => b,
            })
            .collect();
        Cow::Owned(s)
    });
    Ok(Some(ca.into_column()))
}

fn compute_revcomp_rna(s: Column) -> PolarsResult<Option<Column>> {
    let ca = s.str()?;
    let ca = ca.apply_values(|s| {
        let s: String = s
            .chars()
            .rev()
            .map(|b| match b {
                'A' => 'U',
                'U' => 'A',
                'C' => 'G',
                'G' => 'C',
                'a' => 'u',
                'u' => 'a',
                'c' => 'g',
                'g' => 'c',
                _ => b,
            })
            .collect();
        Cow::Owned(s)
    });
    Ok(Some(ca.into_column()))
}

pub fn revcomp<'a>(
    vm: &'a mut Vm,
    args: &Vec<ast::Expr>,
    inplace: bool,
) -> FilterxResult<value::Value> {
    expect_args_len(args, 1)?;

    let col_name = eval_col!(
        vm,
        &args[0],
        "revcomp: expected a column name as first argument"
    );
    if vm.source.source_type.is_fasta() || vm.source.source_type.is_fastq() {
        let name = col_name.column()?;
        let mut e = col_name.expr()?;
        vm.source_mut().has_column(name);
        if vm.source.source_type.is_fasta() {
            let fasta = vm.source.get_fasta()?;
            match fasta.record_type {
                FastaRecordType::Dna => {
                    e = e.map(compute_revcomp_dna, GetOutput::same_type());
                }
                FastaRecordType::Rna => {
                    e = e.map(compute_revcomp_rna, GetOutput::same_type());
                }
                FastaRecordType::Protein => {
                    let h = &mut vm.hint;
                    h.white("revcomp: protein sequences are not supported")
                        .print_and_exit();
                }
                FastaRecordType::Auto => {
                    let h = &mut vm.hint;
                    h.white("revcomp: unknown sequence type.").print_and_exit();
                }
            }
        }

        if inplace {
            vm.source_mut().with_column(e.clone().alias(name), None);
            return Ok(value::Value::None);
        }
        return Ok(value::Value::named_expr(Some(name.to_string()), e));
    } else {
        let h = &mut vm.hint;
        h.white("revcomp: Only fastq and fasta are supported.")
            .print_and_exit()
    }
}
