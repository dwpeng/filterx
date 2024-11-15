use crate::builtin_function;

builtin_function! {
    FUNCTION_SEQUENCE,
    (gc, true),
    (revcomp, true),
    (to_fasta, false, (to_fa)),
    (to_fastq, false, (to_fq)),
    (qual, true),
    (phred, false),
}
