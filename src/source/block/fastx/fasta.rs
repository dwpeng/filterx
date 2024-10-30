use polars::prelude::*;
use std::io::BufRead;

use super::FastaRecordType;
use crate::error::FilterxResult;
use crate::source::block::reader::TableLikeReader;
use crate::source::block::table_like::TableLike;
use crate::util;

#[derive(Debug, Clone, Copy)]
pub struct FastaRecordParserOptions {
    pub include_comment: bool,
}

impl Default for FastaRecordParserOptions {
    fn default() -> Self {
        FastaRecordParserOptions {
            include_comment: true,
        }
    }
}

pub struct FastaSource {
    pub fasta: Fasta,
}

impl FastaSource {
    pub fn new(path: &str, include_comment: bool) -> FilterxResult<Self> {
        let fasta = Fasta::from_path(path)?;
        let opt = FastaRecordParserOptions { include_comment };
        let fasta = fasta.set_parser_options(opt);
        Ok(FastaSource { fasta })
    }

    pub fn into_dataframe(&mut self, n: usize) -> FilterxResult<Option<DataFrame>> {
        let mut records = Vec::with_capacity(n);
        let mut count = 0;
        while let Some(record) = self.fasta.parse_next()? {
            let r = record.clone();
            records.push(r);
            count += 1;

            if count >= n {
                break;
            }
        }
        if records.is_empty() {
            return Ok(None);
        }

        let df = Fasta::as_dataframe(records, &self.fasta.parser_options)?;
        Ok(Some(df))
    }

    pub fn reset(&mut self) -> FilterxResult<()> {
        self.fasta.reset()
    }
}

pub struct Fasta {
    reader: TableLikeReader,
    prev_line: Vec<u8>,
    read_end: bool,
    pub path: String,
    pub parser_options: FastaRecordParserOptions,
    record: FastaRecord,
    pub record_type: FastaRecordType,
}

#[derive(Clone, Debug, Default)]
pub struct FastaRecord {
    buffer: Vec<u8>,
    _name: (usize, usize),
    _comment: (usize, usize),
    _sequence: (usize, usize),
}

impl FastaRecord {
    pub fn new(raw: &str) -> Self {
        FastaRecord {
            buffer: raw.as_bytes().to_vec(),
            _name: (0, 0),
            _comment: (0, 0),
            _sequence: (0, 0),
        }
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.buffer.clear();
        self._name = (0, 0);
        self._comment = (0, 0);
        self._sequence = (0, 0);
    }

    #[inline(always)]
    pub fn format<'a>(&self, s: &'a mut Vec<u8>) -> &'a str {
        s.clear();
        s.extend_from_slice(b">");
        s.extend_from_slice(&self.buffer[self._name.0..self._name.1]);
        if self._comment.0 != self._comment.1 {
            s.extend_from_slice(b" ");
            s.extend_from_slice(&self.buffer[self._comment.0..self._comment.1]);
        }
        s.extend_from_slice(b"\n");
        s.extend_from_slice(&self.buffer[self._sequence.0..self._sequence.1]);
        unsafe { std::str::from_utf8_unchecked(s) }
    }
}

impl FastaRecord {
    #[inline(always)]
    pub fn name(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.buffer[self._name.0..self._name.1]) }
    }

    #[inline(always)]
    pub fn comment(&self) -> Option<&str> {
        if self._comment.0 == self._comment.1 {
            None
        } else {
            let c = unsafe {
                std::str::from_utf8_unchecked(&self.buffer[self._comment.0..self._comment.1])
            };
            Some(c)
        }
    }

    #[inline(always)]
    pub fn seq(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.buffer[self._sequence.0..self._sequence.1]) }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self._sequence.1 - self._sequence.0 + 1
    }
}

impl std::fmt::Display for FastaRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, ">{}", self.name())?;
        if let Some(comment) = self.comment() {
            write!(f, " {}", comment)?;
        }
        write!(f, "\n{}", self.seq())
    }
}

pub struct FastaRecordIter {
    fasta: Fasta,
}

impl Iterator for FastaRecordIter {
    type Item = FastaRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match self.fasta.parse_next() {
            Ok(Some(record)) => Some(record.clone()),
            Ok(None) => None,
            Err(e) => {
                eprintln!("{}", e);
                None
            }
        }
    }
}

impl IntoIterator for Fasta {
    type Item = FastaRecord;
    type IntoIter = FastaRecordIter;

    fn into_iter(self) -> Self::IntoIter {
        FastaRecordIter { fasta: self }
    }
}

impl Clone for Fasta {
    fn clone(&self) -> Self {
        Fasta {
            reader: self.reader.clone(),
            path: self.path.clone(),
            parser_options: self.parser_options.clone(),
            prev_line: self.prev_line.clone(),
            read_end: false,
            record: self.record.clone(),
            record_type: self.record_type.clone(),
        }
    }
}

impl<'a> TableLike<'a> for Fasta {
    type Table = Fasta;
    type Record = FastaRecord;
    type ParserOptions = FastaRecordParserOptions;

    fn from_path(path: &str) -> crate::error::FilterxResult<Self::Table> {
        Ok(Fasta {
            reader: TableLikeReader::new(path)?,
            prev_line: Vec::new(),
            read_end: false,
            path: path.to_string(),
            parser_options: FastaRecordParserOptions::default(),
            record: FastaRecord::default(),
            record_type: FastaRecordType::DNA,
        })
    }

    fn set_parser_options(mut self, parser_options: Self::ParserOptions) -> Self {
        self.parser_options = parser_options;
        self
    }

    fn reset(&mut self) -> FilterxResult<()> {
        self.reader.reset()?;
        self.prev_line.clear();
        self.read_end = false;
        Ok(())
    }

    fn parse_next(&'a mut self) -> FilterxResult<Option<&'a mut FastaRecord>> {
        if self.read_end {
            return Ok(None);
        }
        let record: &mut FastaRecord = &mut self.record;
        record.clear();

        // read name and comment
        if !self.prev_line.is_empty() {
            util::append_vec(&mut record.buffer, &self.prev_line);
        } else {
            let bytes = self.reader.read_until(b'\n', &mut record.buffer)?;
            if bytes == 0 {
                self.read_end = true;
                return Ok(None);
            }
        }

        // fill name and comment
        record._name.0 = 1;
        record._name.1 = record.buffer.len();

        // remove \n or \r\n
        let mut end = record.buffer.len();
        let name = &record.buffer[..];
        if name[name.len() - 1] == b'\n' {
            end -= 1;
            if name[name.len() - 2] == b'\r' {
                end -= 1;
            }
        }
        record.buffer.truncate(end);
        record._name.1 = end;

        if let Some(start) = record.buffer.iter().position(|&x| x == b' ') {
            record._name.1 = start;
            if self.parser_options.include_comment {
                record._comment.0 = start + 1;
                record._comment.1 = record.buffer.len();
            } else {
                record.buffer.truncate(start);
                record._comment.0 = 0;
                record._comment.1 = 0;
            }
        }

        // fill sequence
        self.prev_line.clear();
        record._sequence.0 = record.buffer.len();
        loop {
            let bytes = self.reader.read_until(b'\n', &mut self.prev_line)?;
            if bytes == 0 {
                self.read_end = true;
                break;
            }
            if self.prev_line[0] == b'>' {
                break;
            }
            // remove \n or \r\n
            let mut line = self.prev_line.as_slice();
            if line[line.len() - 1] == b'\n' {
                line = &line[..line.len() - 1];
                if line[line.len() - 1] == b'\r' {
                    line = &line[..line.len() - 1];
                }
            }
            util::append_vec(&mut record.buffer, line);
            self.prev_line.clear();
        }
        if record.buffer.is_empty() {
            return Ok(None);
        }
        record._sequence.1 = record.buffer.len();
        Ok(Some(record))
    }

    fn into_dataframe(self) -> crate::error::FilterxResult<polars::prelude::DataFrame> {
        let mut names: Vec<String> = Vec::new();
        let mut sequences: Vec<String> = Vec::new();
        let mut comments: Vec<String> = Vec::new();
        let mut fasta = self;

        loop {
            match fasta.parse_next()? {
                Some(record) => {
                    names.push(record.name().into());
                    sequences.push(record.seq().into());
                    if let Some(comment) = record.comment() {
                        comments.push(comment.into());
                    }
                }
                None => break,
            }
        }
        let mut cols = Vec::with_capacity(3);
        cols.append(&mut vec![
            polars::prelude::Series::new("name".into(), names),
            polars::prelude::Series::new("seq".into(), sequences),
        ]);
        if !comments.is_empty() {
            cols.push(polars::prelude::Series::new("comment".into(), comments));
        }
        let df = polars::prelude::DataFrame::new(cols)?;

        Ok(df)
    }

    fn as_dataframe(
        records: Vec<FastaRecord>,
        parser_options: &Self::ParserOptions,
    ) -> crate::error::FilterxResult<polars::prelude::DataFrame> {
        let mut headers: Vec<String> = Vec::new();
        let mut sequences: Vec<String> = Vec::new();
        let mut comments: Vec<String> = Vec::new();
        for record in records {
            headers.push(record.name().into());
            sequences.push(record.seq().into());
            if let Some(comment) = record.comment() {
                comments.push(comment.into());
            } else {
                if parser_options.include_comment {
                    comments.push("".into());
                }
            }
        }
        let mut cols = vec![
            polars::prelude::Series::new("name".into(), headers),
            polars::prelude::Series::new("seq".into(), sequences),
        ];
        if comments.len() > 0 {
            cols.push(polars::prelude::Series::new("comment".into(), comments));
        }
        let df = polars::prelude::DataFrame::new(cols)?;
        Ok(df)
    }
}

pub mod test {

    #[allow(unused)]
    use super::*;

    #[test]
    fn test_open_plain_file() -> FilterxResult<()> {
        let path = "test_data/fasta/1.fa";
        let fasta = Fasta::from_path(path)?;
        let records = fasta.into_iter().collect::<Vec<FastaRecord>>();
        assert!(records.len() == 2);
        Ok(())
    }

    #[test]
    fn test_open_gzip_file() -> FilterxResult<()> {
        let path = "test_data/fasta/1.fa.gz";
        let fasta = Fasta::from_path(path)?;
        let records = fasta.into_iter().collect::<Vec<FastaRecord>>();
        assert!(records.len() == 2);
        Ok(())
    }
}
