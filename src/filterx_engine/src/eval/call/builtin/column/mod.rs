use crate::builtin_function;

builtin_function! {
    FUNCTION_COLUMN,
    (rm, false, false),
    (alias,false, false),
    (cast, true, true, (
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
    (dup,false, false, (dup_any, dup_none, dup_last)),
    (fill, true, true, (fill_null, fill_nan)),
    (is_null, false, false, (is_not_null)),
    (rename, false, false),
    (select, false, false),
    (print, false, false, (format, fmt, f)),
    (sort, false, false, (Sort, sorT)),
    (col, true, false, (c)),
    (drop_null, false, false),
    (header, false, false),
    (occ, false, false, (occ_lte, occ_gte)),
}
