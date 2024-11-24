use crate::builtin_function;

builtin_function! {
    FUNCTION_SEQUENCE,
    (gc, true, false),
    (revcomp, true, true),
    (to_fasta, false, false, (to_fa)),
    (to_fastq, false, false, (to_fq)),
    (qual, true, false),
    (phred, false, false),
    (hpc, true, true),
}
