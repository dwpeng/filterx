use super::*;

pub fn header(vm: &mut Vm) -> FilterxResult<value::Value> {
    let source = &vm.source;
    let schema = source.columns()?;
    for (index, (name, t)) in schema.iter().enumerate() {
        println!("{}\t{}\t{}", index, name, t);
    }
    std::process::exit(0);
}
