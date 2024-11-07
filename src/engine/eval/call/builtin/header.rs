use super::*;

pub fn header(vm: &mut Vm) -> FilterxResult<value::Value> {
    let source = &vm.source;
    let schema = source.columns()?;
    for (name, t) in schema {
        println!("{}\t{}", name, t);
    }
    std::process::exit(0);
}
