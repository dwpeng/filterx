use crate::dataframe::DataframeSource;
use filterx_core::{
    reader::{detect_breakline_len, FilterxReader},
    FilterxResult, Hint,
};
use memchr::memchr;
use polars::prelude::*;
use std::{collections::HashSet, io::BufRead};

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
    pub dataframe: DataframeSource,
}

impl Drop for FastaSource {
    fn drop(&mut self) {
        unsafe {
            self.records.set_len(self.records.capacity());
        }
    }
}

impl FastaSource {
    pub fn new(
        path: &str,
        include_comment: bool,
        record_type: FastaRecordType,
        n_detect: usize,
    ) -> FilterxResult<Self> {
        let fasta = Fasta::from_path(path, record_type, n_detect)?;
        let opt = FastaParserOptions { include_comment };
        let fasta = fasta.set_parser_options(opt);
        let records = vec![FastaRecord::default(); 4096];
        let dataframe = DataframeSource::new(DataFrame::empty().lazy());
        Ok(FastaSource {
            fasta,
            records,
            dataframe,
        })
    }

    pub fn into_dataframe(&mut self, n: usize) -> FilterxResult<usize> {
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
            return Ok(0);
        }
        let df = Fasta::as_dataframe(&records, &self.fasta.parser_options)?;
        self.dataframe.update(df.lazy());
        Ok(count)
    }

    pub fn reset(&mut self) -> FilterxResult<()> {
        self.fasta.reset()
    }
}

pub struct Fasta {
    reader: FilterxReader,
    read_end: bool,
    pub path: String,
    pub parser_options: FastaParserOptions,
    record: FastaRecord,
    pub record_type: FastaRecordType,
    break_line_len: Option<usize>,
    buffer_unprocess_size: usize,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum, PartialEq)]
pub enum FastaRecordType {
    Dna,
    Rna,
    Protein,
    Auto,
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

    pub fn goto_next_record(&mut self, left: usize) {
        if self.buffer.len() < self.buffer.capacity() && left > 0 {
            unsafe {
                std::ptr::copy(
                    self.buffer.as_ptr().add(self.buffer.len()),
                    self.buffer.as_mut_ptr(),
                    left,
                );
                self.buffer.set_len(left);
            }
        }
        self._comment = (0, 0);
        self._name = (0, 0);
        self._sequence = (0, 0);
    }

    pub fn remove_breakline_from_buffer(&mut self, len: usize) {
        if len > 0 {
            unsafe {
                self.buffer.set_len(self.buffer.len() - len);
            }
        }
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

impl Clone for Fasta {
    fn clone(&self) -> Self {
        Fasta {
            reader: self.reader.clone(),
            path: self.path.clone(),
            parser_options: self.parser_options.clone(),
            read_end: false,
            record: self.record.clone(),
            record_type: self.record_type.clone(),
            break_line_len: self.break_line_len.clone(),
            buffer_unprocess_size: self.buffer_unprocess_size,
        }
    }
}

impl Fasta {
    pub fn from_path(
        path: &str,
        record_type: FastaRecordType,
        n_detect: usize,
    ) -> FilterxResult<Fasta> {
        let mut fasta = Fasta {
            reader: FilterxReader::new(path)?,
            read_end: false,
            path: path.to_string(),
            parser_options: FastaParserOptions::default(),
            record: FastaRecord::default(),
            record_type,
            break_line_len: None,
            buffer_unprocess_size: 0,
        };
        fasta.break_line_len = detect_breakline_len(&mut fasta.reader)?;
        if record_type == FastaRecordType::Auto {
            fasta.detect_record_type(n_detect)?;
        }
        Ok(fasta)
    }

    pub fn detect_record_type(&mut self, n: usize) -> FilterxResult<()> {
        let mut hashset = HashSet::new();
        for _ in 0..n {
            let record = self.parse_next()?;
            if record.is_none() {
                break;
            }
            let seq = record.unwrap().seq();
            for c in seq.bytes() {
                if c == b'n' || c == b'N' {
                    continue;
                }
                hashset.insert(c);
            }
        }
        if hashset.len() < 4 {
            let mut h = Hint::new();
            h.white("Too less sequences are used to detect alphabet. Try increase the number of sequences to detect alphabet.")
                .print_and_exit();
        }
        if hashset.len() > 4 {
            self.record_type = FastaRecordType::Protein;
        }
        let contain_t = hashset.contains(&b'T') || hashset.contains(&b't');
        let contain_u = hashset.contains(&b'u') || hashset.contains(&b'U');
        if contain_t && contain_u {
            let mut h = Hint::new();
            h.white("The fasta file contains both ")
                .cyan("'T'")
                .white(" and ")
                .cyan("'U'")
                .white(" nucleotides. Can not determine the record type.")
                .print_and_exit();
        }
        if !contain_t && !contain_u {
            let mut h = Hint::new();
            h.white("The fasta file contains none of ")
                .cyan("'T'")
                .white(" and ")
                .cyan("'U'")
                .white(" nucleotides. Can not determine the record type.")
                .print_and_exit();
        }
        if contain_t {
            self.record_type = FastaRecordType::Dna;
        } else if contain_u {
            self.record_type = FastaRecordType::Rna;
        }
        self.reset()?;
        Ok(())
    }

    pub fn goto_next_record(&mut self) {
        self.record.goto_next_record(self.buffer_unprocess_size);
        self.buffer_unprocess_size = 0;
    }

    pub fn set_parser_options(mut self, parser_options: FastaParserOptions) -> Self {
        self.parser_options = parser_options;
        self
    }

    pub fn reset(&mut self) -> FilterxResult<()> {
        self.reader.reset()?;
        self.record.clear();
        self.read_end = false;
        self.buffer_unprocess_size = 0;
        Ok(())
    }

    pub fn parse_next(&mut self) -> FilterxResult<Option<&mut FastaRecord>> {
        if self.read_end {
            return Ok(None);
        }
        self.goto_next_record();
        let record: &mut FastaRecord = &mut self.record;
        // read name and comment
        if record.buffer.is_empty() {
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

        let break_line_len = self.break_line_len.unwrap();

        // fill name and comment
        record._name.0 = 1;
        record._name.1 = record.buffer.len() - break_line_len;

        let start = memchr(b' ', &record.buffer[1..record._name.1]);

        if let Some(mut start) = start {
            start += 1;
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
        record._sequence.0 = record.buffer.len();
        loop {
            let buffer_offset = record.buffer.len();
            let bytes = self.reader.read_until(b'\n', &mut record.buffer)?;
            if bytes == 0 {
                self.read_end = true;
                break;
            }
            if record.buffer[buffer_offset] == b'>' {
                unsafe {
                    record.buffer.set_len(buffer_offset);
                }
                self.buffer_unprocess_size = bytes;
                break;
            }
            record.remove_breakline_from_buffer(break_line_len);
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
