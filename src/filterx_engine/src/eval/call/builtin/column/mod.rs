use crate::builtin_function;

builtin_function! {
    FUNCTION_COLUMN,
    (drop, false),
    (alias,false),
    (cast, true, (
        cast_int,
        cast_float,
        cast_string,
        cast_str,
        cast_bool,
        cast_i32,
        cast_i64,
        cast_f32,
        cast_f64,
        cast_i16,
        cast_i8,
        cast_u16,
        cast_u8
    )),
    (dup,false),
    (fill, true),
    (is_na, false),
    (is_null, false),
    (rename, false),
    (select, false),
    (print, false),
    (sort, false),
    (col, true),
    (drop_null, false),
    (header, false),
}
