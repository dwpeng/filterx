use polars::prelude::*;
use std::io::BufRead;

use crate::error::FilterxResult;
use crate::source::block::reader::TableLikeReader;
use crate::source::block::table_like::TableLike;

pub struct FastqSource {
    pub fastq: Fastq,
}

impl FastqSource {
    pub fn new(path: &str, include_comment: bool, include_qual: bool) -> FilterxResult<Self> {
        let parser_option = FastqParserOption {
            include_comment,
            include_qual,
        };
        let fastq = Fastq::from_path(path)?.set_parser_options(parser_option);
        Ok(FastqSource { fastq })
    }

    pub fn into_dataframe(&mut self, n: usize) -> FilterxResult<Option<DataFrame>> {
        let mut records = Vec::with_capacity(n);
        let mut count = 0;
        while let Some(record) = self.fastq.parse_next()? {
            let r = record.clone();
            records.push(r);
            count += 1;
            if count >= n {
                break;
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            let df = Fastq::as_dataframe(records, &self.fastq.parser_option)?;
            Ok(Some(df))
        }
    }

    pub fn reset(&mut self) -> FilterxResult<()> {
        self.fastq.reset()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FastqParserOption {
    pub include_qual: bool,
    pub include_comment: bool,
}

impl Default for FastqParserOption {
    fn default() -> Self {
        FastqParserOption {
            include_qual: true,
            include_comment: true,
        }
    }
}

pub struct Fastq {
    reader: TableLikeReader,
    line_buffer: String,
    read_end: bool,
    pub path: String,
    pub parser_option: FastqParserOption,
    record: FastqRecord,
}

#[derive(Debug, Default, Clone)]
pub struct FastqRecord {
    buffer: String,
    _name: (usize, usize),
    _sequence: (usize, usize),
    _qual: (usize, usize),
    _comment: (usize, usize),
}

impl std::fmt::Display for FastqRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "@{}", self.name())?;
        if let Some(comment) = self.comment() {
            write!(f, " {}", comment)?;
        }
        write!(f, "\n{}\n", self.seq())?;
        if let Some(qual) = self.qual() {
            write!(f, "+\n{}", qual)?;
        }
        Ok(())
    }
}

impl FastqRecord {
    pub fn new() -> Self {
        FastqRecord {
            buffer: String::new(),
            _name: (0, 0),
            _sequence: (0, 0),
            _qual: (0, 0),
            _comment: (0, 0),
        }
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self._name = (0, 0);
        self._sequence = (0, 0);
        self._qual = (0, 0);
        self._comment = (0, 0);
    }
}

impl FastqRecord {
    pub fn name(&self) -> &str {
        &self.buffer[self._name.0..self._name.1]
    }

    pub fn comment(&self) -> Option<&str> {
        if self._comment.0 == self._comment.1 {
            None
        } else {
            Some(&self.buffer[self._comment.0..self._comment.1])
        }
    }

    pub fn seq(&self) -> &str {
        &self.buffer[self._sequence.0..self._sequence.1]
    }

    pub fn len(&self) -> usize {
        self._sequence.1 - self._sequence.0 + 1
    }

    pub fn qual(&self) -> Option<&str> {
        if self._qual.0 == self._qual.1 {
            None
        } else {
            Some(&self.buffer[self._qual.0..self._qual.1])
        }
    }
}

pub struct FastqRecordIter {
    fastq: Fastq,
}

impl Iterator for FastqRecordIter {
    type Item = FastqRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match self.fastq.parse_next() {
            Ok(Some(record)) => Some(record.clone()),
            Ok(None) => None,
            Err(e) => {
                eprintln!("{}", e);
                None
            }
        }
    }
}

impl IntoIterator for Fastq {
    type Item = FastqRecord;
    type IntoIter = FastqRecordIter;

    fn into_iter(self) -> Self::IntoIter {
        FastqRecordIter { fastq: self }
    }
}

impl<'a> TableLike<'a> for Fastq {
    type ParserOptions = FastqParserOption;
    type Record = FastqRecord;
    type Table = Fastq;

    fn from_path(path: &str) -> FilterxResult<Self::Table> {
        Ok(Fastq {
            reader: TableLikeReader::new(path)?,
            line_buffer: String::new(),
            read_end: false,
            parser_option: FastqParserOption::default(),
            path: path.to_string(),
            record: FastqRecord::default(),
        })
    }

    fn parse_next(&'a mut self) -> FilterxResult<Option<&'a mut Self::Record>> {
        if self.read_end {
            return Ok(None);
        }
        let record = &mut self.record;
        record.clear();

        if self.line_buffer.len() == 0 {
            let bytes = self.reader.read_line(&mut self.line_buffer)?;
            if bytes == 0 {
                self.read_end = true;
                return Ok(None);
            }
        }
        let line = self.line_buffer.trim_end();
        if line.starts_with('@') {
            record._name.0 = 1;
            record.buffer.push_str(line);
            record._name.1 = record.buffer.len();
            let comment_start = line.find(' ');
            if let Some(start) = comment_start {
                record._name.1 = start;
                if self.parser_option.include_comment {
                    record._comment.0 = start + 1;
                    record._comment.1 = record.buffer.len();
                } else {
                    record.buffer.truncate(start);
                }
            }
        } else {
            return Err(crate::FilterxError::FastqError(
                "Invalid fastq format".to_string(),
            ));
        }
        self.line_buffer.clear();
        let bytes = self.reader.read_line(&mut self.line_buffer)?;
        if bytes == 0 {
            self.read_end = true;
            return Err(crate::FilterxError::FastqError(
                "Invalid fastq format".to_string(),
            ));
        }
        let line = self.line_buffer.trim_end();
        record._sequence.0 = record.buffer.len();
        record.buffer.push_str(line);
        record._sequence.1 = record.buffer.len();
        self.line_buffer.clear();
        let bytes = self.reader.read_line(&mut self.line_buffer)?;
        if bytes == 0 {
            self.read_end = true;
            return Err(crate::FilterxError::FastqError(
                "Invalid fastq format".to_string(),
            ));
        }
        let line = self.line_buffer.trim_end();
        if !line.starts_with('+') {
            return Err(crate::FilterxError::FastqError(
                "Invalid fastq format".to_string(),
            ));
        }
        self.line_buffer.clear();
        let bytes = self.reader.read_line(&mut self.line_buffer)?;
        if bytes == 0 {
            self.read_end = true;
            return Err(crate::FilterxError::FastqError(
                "Invalid fastq format".to_string(),
            ));
        }
        if self.parser_option.include_qual {
            let line = self.line_buffer.trim_end();
            record._qual.0 = record.buffer.len();
            record.buffer.push_str(line);
            record._qual.1 = record.buffer.len();
        }
        self.line_buffer.clear();
        Ok(Some(record))
    }

    fn set_parser_options(self, parser_options: Self::ParserOptions) -> Self {
        let mut fastq = self;
        fastq.parser_option = parser_options;
        fastq
    }

    fn into_dataframe(self) -> FilterxResult<DataFrame> {
        let mut names: Vec<String> = Vec::new();
        let mut sequences: Vec<String> = Vec::new();
        let mut quals: Vec<String> = Vec::new();
        let mut comments: Vec<String> = Vec::new();
        let mut fastq = self;

        loop {
            match fastq.parse_next()? {
                Some(record) => {
                    names.push(record.name().to_string());
                    sequences.push(record.seq().to_string());
                    if let Some(qual) = record.qual() {
                        quals.push(qual.to_string());
                    }
                    if let Some(comment) = record.comment() {
                        comments.push(comment.to_string());
                    }
                }
                None => break,
            }
        }

        let mut cols = Vec::new();
        cols.push(Series::new("name".into(), &names));
        cols.push(Series::new("seq".into(), &sequences));
        if !quals.is_empty() {
            cols.push(Series::new("qual".into(), &quals));
        }
        if !comments.is_empty() {
            cols.push(Series::new("comm".into(), &comments));
        }
        let df = DataFrame::new(cols)?;
        Ok(df)
    }

    fn as_dataframe(
        records: Vec<Self::Record>,
        parser_options: &Self::ParserOptions,
    ) -> FilterxResult<DataFrame> {
        let mut names: Vec<String> = Vec::new();
        let mut sequences: Vec<String> = Vec::new();
        let mut quals: Vec<String> = Vec::new();
        let mut comments: Vec<String> = Vec::new();
        for record in records {
            names.push(record.name().to_string());
            sequences.push(record.seq().to_string());
            if let Some(qual) = record.qual() {
                quals.push(qual.to_string());
            } else {
                if parser_options.include_qual {
                    quals.push("".to_string());
                }
            }
            if let Some(comment) = record.comment() {
                comments.push(comment.to_string());
            } else {
                if parser_options.include_comment {
                    comments.push("".to_string());
                }
            }
        }

        let mut cols = Vec::new();
        cols.push(Series::new("name".into(), &names));
        if !comments.is_empty() {
            cols.push(Series::new("comm".into(), &comments));
        }
        cols.push(Series::new("seq".into(), &sequences));
        if !quals.is_empty() {
            cols.push(Series::new("qual".into(), &quals));
        }

        let df = DataFrame::new(cols)?;
        Ok(df)
    }

    fn reset(&mut self) -> FilterxResult<()> {
        self.reader.reset()?;
        self.line_buffer.clear();
        self.read_end = false;
        Ok(())
    }
}
