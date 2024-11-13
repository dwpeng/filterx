use polars::prelude::*;
use std::io::BufRead;

use super::FastaRecordType;
use crate::block::reader::TableLikeReader;

use filterx_core::{FilterxResult, Hint};

#[derive(Debug, Clone, Copy)]
pub struct FastaParserOptions {
    pub include_comment: bool,
}

impl Default for FastaParserOptions {
    fn default() -> Self {
        FastaParserOptions {
            include_comment: true,
        }
    }
}

pub struct FastaSource {
    pub fasta: Fasta,
    pub records: Vec<FastaRecord>,
}

impl Drop for FastaSource {
    fn drop(&mut self) {
        unsafe {
            self.records.set_len(self.records.capacity());
        }
    }
}

impl FastaSource {
    pub fn new(path: &str, include_comment: bool) -> FilterxResult<Self> {
        let fasta = Fasta::from_path(path)?;
        let opt = FastaParserOptions { include_comment };
        let fasta = fasta.set_parser_options(opt);
        let records = vec![FastaRecord::default(); 4096];
        Ok(FastaSource { fasta, records })
    }

    pub fn into_dataframe(&mut self, n: usize) -> FilterxResult<Option<DataFrame>> {
        let records = &mut self.records;
        if records.capacity() < n {
            unsafe {
                records.set_len(records.capacity());
            }
            for _ in records.capacity()..=n {
                records.push(FastaRecord::default());
            }
        }
        unsafe {
            records.set_len(n);
        }
        let mut count = 0;
        while let Some(record) = self.fasta.parse_next()? {
            let r = unsafe { records.get_unchecked_mut(count) };
            r.clear();
            r.buffer.extend_from_slice(&record.buffer);
            r._name = record._name;
            r._sequence = record._sequence;
            r._comment = record._comment;
            count += 1;
            if count >= n {
                break;
            }
        }
        unsafe {
            records.set_len(count);
        }
        if records.is_empty() {
            return Ok(None);
        }

        let df = Fasta::as_dataframe(&records, &self.fasta.parser_options)?;
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
    pub parser_options: FastaParserOptions,
    record: FastaRecord,
    pub record_type: FastaRecordType,
    break_line_len: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct FastaRecord {
    buffer: Vec<u8>,
    _name: (usize, usize),
    _comment: (usize, usize),
    _sequence: (usize, usize),
}

impl Default for FastaRecord {
    fn default() -> Self {
        FastaRecord {
            buffer: Vec::with_capacity(128),
            _name: (0, 0),
            _comment: (0, 0),
            _sequence: (0, 0),
        }
    }
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
    pub fn format<'a>(&'a self) -> &'a str {
        unsafe { std::str::from_utf8_unchecked(&self.buffer) }
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
            break_line_len: self.break_line_len.clone(),
        }
    }
}

impl Fasta {
    pub fn from_path(path: &str) -> FilterxResult<Fasta> {
        Ok(Fasta {
            reader: TableLikeReader::new(path)?,
            prev_line: Vec::new(),
            read_end: false,
            path: path.to_string(),
            parser_options: FastaParserOptions::default(),
            record: FastaRecord::default(),
            record_type: FastaRecordType::DNA,
            break_line_len: None,
        })
    }

    pub fn set_parser_options(mut self, parser_options: FastaParserOptions) -> Self {
        self.parser_options = parser_options;
        self
    }

    pub fn reset(&mut self) -> FilterxResult<()> {
        self.reader.reset()?;
        self.prev_line.clear();
        self.read_end = false;
        Ok(())
    }

    pub fn parse_next(&mut self) -> FilterxResult<Option<&mut FastaRecord>> {
        if self.read_end {
            return Ok(None);
        }
        let record: &mut FastaRecord = &mut self.record;
        record.clear();
        // read name and comment
        if !self.prev_line.is_empty() {
            record.buffer.extend_from_slice(&self.prev_line);
        } else {
            let bytes = self.reader.read_until(b'\n', &mut record.buffer)?;
            if bytes == 0 {
                self.read_end = true;
                return Ok(None);
            }
        }

        if record.buffer[0] != b'>' {
            let mut h = Hint::new();
            h.white("Invalid FASTA format. Expecting ")
                .cyan(">")
                .bold()
                .white(" at the beginning of the line, but got: ")
                .cyan(unsafe { std::str::from_utf8_unchecked(&record.buffer[0..1]) })
                .bold()
                .white(". ");
            if record.buffer[0] == b'@' {
                h.white("This looks like a FASTQ file. Plaease try ")
                    .green("filterx fastq")
                    .bold()
                    .white(" command instead.");
            }
            h.print_and_exit();
        }

        // fill name and comment
        record._name.0 = 1;
        record._name.1 = record.buffer.len();

        // remove \n or \r\n
        let end = record.buffer.len();
        let break_line_len;
        if self.break_line_len.is_some() {
            break_line_len = self.break_line_len.unwrap();
        } else {
            let name = &record.buffer[..];
            if name.ends_with(&[b'\n', b'\r']) {
                break_line_len = 2;
            } else {
                break_line_len = 1;
            }
            self.break_line_len = Some(break_line_len);
        }
        record._name.1 = end - break_line_len;

        let mut start = None;

        for i in 0..record._name.1 {
            if record.buffer[i] == b' ' {
                start = Some(i);
                break;
            }
        }

        if let Some(start) = start {
            record._name.1 = start;
            if self.parser_options.include_comment {
                record._comment.0 = start + 1;
                record._comment.1 = record.buffer.len() - break_line_len;
            } else {
                record.buffer[start] = b'\n';
                record.buffer.truncate(start + 1);
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
            let line = &self.prev_line[..bytes - break_line_len];
            record.buffer.extend_from_slice(line);
            self.prev_line.clear();
        }
        if record.buffer.is_empty() {
            return Ok(None);
        }
        record._sequence.1 = record.buffer.len();
        Ok(Some(record))
    }

    pub fn as_dataframe(
        records: &Vec<FastaRecord>,
        parser_options: &FastaParserOptions,
    ) -> FilterxResult<polars::prelude::DataFrame> {
        let mut headers: Vec<&str> = Vec::with_capacity(records.len());
        let mut sequences: Vec<&str> = Vec::with_capacity(records.len());
        let mut comments: Vec<&str> = if parser_options.include_comment {
            Vec::with_capacity(records.len())
        } else {
            Vec::with_capacity(0)
        };

        for record in records {
            headers.push(record.name());
            sequences.push(record.seq());
            if let Some(comment) = record.comment() {
                comments.push(comment);
            } else {
                if parser_options.include_comment {
                    comments.push("");
                }
            }
        }

        let mut cols = Vec::with_capacity(3);
        cols.push(polars::prelude::Column::new("name".into(), headers));
        if comments.len() > 0 {
            cols.push(polars::prelude::Column::new("comm".into(), comments));
        }
        cols.push(polars::prelude::Column::new("seq".into(), sequences));
        let df = DataFrame::new(cols)?;
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
