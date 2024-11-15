use super::builtin::*;

pub static ALL_FUNCTIONS: [&'static [Builtin]; 5] = [
    FUNCTION_COLUMN,
    FUNCTION_STRING,
    FUNCTION_SEQUENCE,
    FUNCTION_NUMBER,
    FUNCTION_ROW,
];

pub fn get_function(name: &str) -> Option<&'static Builtin> {
    for functions in ALL_FUNCTIONS.iter() {
        for function in functions.iter() {
            if name == function.name || function.alias.contains(&name) {
                return Some(function);
            }
        }
    }
    None
}
