use super::super::*;
pub fn phred(vm: &mut Vm) -> FilterxResult<value::Value> {
    if vm.source_type() == SourceType::Fastq {
        let fastp = vm.source.get_fastq()?;
        let h = &mut vm.hint;
        h.white("phred: ")
            .green(&format!("{}", fastp.quality_type))
            .print_and_exit();
    }
    let h = &mut vm.hint;
    h.white("phred: Only ")
        .cyan("fastq")
        .white(" format is supported for now.")
        .print_and_exit();
}
