use crate::{FilterxError, FilterxResult};

#[derive(Debug, Clone)]
pub struct Separator<'a> {
    pub sep: &'a str,
}

static SEPARATORS: [(&str, char); 2] = [("\\t", '\t'), ("\t", '\t')];

impl<'a> Separator<'a> {
    pub fn new(sep: &'a str) -> Self {
        Separator { sep }
    }
}

impl<'a> Separator<'a> {
    pub fn get_sep(&self) -> FilterxResult<u8> {
        if self.sep.len() == 1 {
            let first = self.sep.as_bytes()[0];
            match first {
                b't' => return Ok('\t' as u8),
                b'\t' => return Ok('\t' as u8),
                b'n' => return Ok('\n' as u8),
                b'\n' => return Ok('\n' as u8),
                b',' => return Ok(',' as u8),
                _ => return Ok(first as u8),
            }
        }
        if self.sep.starts_with("\\") {
            for (s, c) in SEPARATORS.iter() {
                if s == &self.sep {
                    return Ok(*c as u8);
                }
            }
            return Ok(',' as u8);
        }

        Err(FilterxError::RuntimeError(format!(
            "invalid separator: {}",
            self.sep
        )))
    }
}
