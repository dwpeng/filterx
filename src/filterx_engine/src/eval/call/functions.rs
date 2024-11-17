use super::builtin::*;

pub struct BuiltinFunction {
    pub name: &'static str,
    pub alias: &'static [&'static str],
    pub can_expression: bool,
    pub doc: &'static str,
}

pub static ALL_FUNCTIONS: [&'static [BuiltinFunction]; 5] = [
    FUNCTION_COLUMN,
    FUNCTION_STRING,
    FUNCTION_SEQUENCE,
    FUNCTION_NUMBER,
    FUNCTION_ROW,
];

pub fn get_function(name: &str) -> Option<&'static BuiltinFunction> {
    for functions in ALL_FUNCTIONS.iter() {
        for function in functions.iter() {
            if name == function.name || function.alias.contains(&name) {
                return Some(function);
            }
        }
    }
    None
}
