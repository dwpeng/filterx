use polars::prelude::*;
use std::io::BufRead;

use crate::error::FilterxResult;
use crate::hint::Hint;
use crate::source::block::reader::TableLikeReader;
use crate::source::block::table_like::TableLike;
use crate::util;

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
    read_end: bool,
    pub path: String,
    pub parser_option: FastqParserOption,
    record: FastqRecord,
    pub line_buffer: Vec<u8>,
}

#[derive(Debug, Default, Clone)]
pub struct FastqRecord {
    buffer: Vec<u8>,
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
            buffer: Vec::new(),
            _name: (0, 0),
            _sequence: (0, 0),
            _qual: (0, 0),
            _comment: (0, 0),
        }
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.buffer.clear();
        self._name = (0, 0);
        self._sequence = (0, 0);
        self._qual = (0, 0);
        self._comment = (0, 0);
    }

    #[inline(always)]
    pub fn format<'a>(&self, b: &'a mut Vec<u8>) -> &'a str {
        b.clear();
        b.extend_from_slice(b"@");
        b.extend_from_slice(&self.buffer[self._name.0..self._name.1]);
        if self._comment.0 != self._comment.1 {
            b.extend_from_slice(b" ");
            b.extend_from_slice(&self.buffer[self._comment.0..self._comment.1]);
        }
        b.extend_from_slice(b"\n");
        b.extend_from_slice(&self.buffer[self._sequence.0..self._sequence.1]);
        b.extend_from_slice(b"\n");
        if self._qual.0 != self._qual.1 {
            b.extend_from_slice(b"+\n");
            b.extend_from_slice(&self.buffer[self._qual.0..self._qual.1]);
        }
        unsafe { std::str::from_utf8_unchecked(b) }
    }
}

impl FastqRecord {
    #[inline(always)]
    pub fn name(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.buffer[self._name.0..self._name.1]) }
    }

    #[inline(always)]
    pub fn comment(&self) -> Option<&str> {
        if self._comment.0 == self._comment.1 {
            None
        } else {
            unsafe {
                Some(std::str::from_utf8_unchecked(
                    &self.buffer[self._comment.0..self._comment.1],
                ))
            }
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

    #[inline(always)]
    pub fn qual(&self) -> Option<&str> {
        if self._qual.0 == self._qual.1 {
            None
        } else {
            unsafe {
                Some(std::str::from_utf8_unchecked(
                    &self.buffer[self._qual.0..self._qual.1],
                ))
            }
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
            read_end: false,
            parser_option: FastqParserOption::default(),
            path: path.to_string(),
            record: FastqRecord::default(),
            line_buffer: Vec::with_capacity(512),
        })
    }

    // fn parse_next(&'a mut self) -> FilterxResult<Option<&'a mut Self::Record>> {
    //     if self.read_end {
    //         return Ok(None);
    //     }
    //     let record = &mut self.record;
    //     let parser_option = &self.parser_option;
    //     loop {
    //         record.clear();
    //         let bytes = self.reader.read_until(b'@', &mut record.buffer)?;
    //         if bytes == 0 {
    //             self.read_end = true;
    //             return Ok(None);
    //         }
    //         if bytes == 1 {
    //             // first one is @, skip it
    //             continue;
    //         }
    //         // record buff have store name and comment and sequence and qual, as follow:
    //         // \r\n|\n means the end of line is \r\n or \n
    //         // name comment\r\n|\n
    //         // sequence\r\n|\n
    //         // +\r\n|\n
    //         // qual\r\n|\n
    //         // @

    //         // find the end of name and comment
    //         let mut i = 1;
    //         let mut line_end = 0;
    //         let mut space = 0;
    //         let mut have_r = false;
    //         while i < bytes {
    //             if record.buffer[i] == b'\n' {
    //                 line_end = i;
    //                 if record.buffer[i - 1] == b'\r' {
    //                     line_end -= 1;
    //                     have_r = true;
    //                 }
    //                 break;
    //             }
    //             if record.buffer[i] == b' ' {
    //                 space = i;
    //             }
    //             i += 1;
    //         }
    //         if space == 0 {
    //             space = line_end;
    //         }
    //         if i == bytes {
    //             return Err(crate::FilterxError::FastqError(
    //                 "Invalid fastq format: name and comment".to_string(),
    //             ));
    //         }

    //         record._name = (0, space);
    //         if space != line_end {
    //             if parser_option.include_comment {
    //                 record._name = (0, space);
    //                 record._comment = (space + 1, line_end);
    //             }
    //         }

    //         // find the end of sequence
    //         let mut j = line_end + if have_r { 2 } else { 1 };
    //         record._sequence.0 = j;
    //         while j < bytes {
    //             if record.buffer[j] == b'+' {
    //                 break;
    //             }
    //             j += 1;
    //         }
    //         if j == bytes {
    //             return Err(crate::FilterxError::FastqError(
    //                 "Invalid fastq format: sequence".to_string(),
    //             ));
    //         }
    //         record._sequence.1 = j - if have_r { 2 } else { 1 };
    //         if parser_option.include_qual {
    //             let seq_len = record.len();
    //             j = j + if have_r { 2 } else { 1 };
    //             // find the end of qual
    //             // check if qual is equal to sequence
    //             if j + seq_len > bytes {
    //                 return Err(crate::FilterxError::FastqError(
    //                     "Invalid fastq format: qual".to_string(),
    //                 ));
    //             }
    //             record._qual = (j + 1, j + seq_len);
    //         }
    //         return Ok(Some(record));
    //     }
    // }

    /// parse fastq format based paper: https://academic.oup.com/nar/article/38/6/1767/3112533
    fn parse_next(&'a mut self) -> FilterxResult<Option<&'a mut Self::Record>> {
        if self.read_end {
            return Ok(None);
        }
        let record = &mut self.record;
        let mut line_buff = &mut self.line_buffer;
        record.clear();
        if !line_buff.is_empty() {
            util::append_vec(&mut record.buffer, line_buff);
        } else {
            let bytes = self.reader.read_until(b'\n', &mut record.buffer)?;
            if bytes == 0 {
                self.read_end = true;
                return Ok(None);
            }
        }

        if record.buffer[0] != b'@' {
            let mut h = Hint::new();
            h.white("Invalid FASTQ format. Expecting ")
                .cyan("@")
                .bold()
                .white(" at the beginning of the line, but got: ")
                .cyan(unsafe { std::str::from_utf8_unchecked(&record.buffer[0..1]) })
                .bold()
                .white(". ");
            if record.buffer[0] == b'>' {
                h.white("This looks like a FASTA file. Plaease try ")
                    .green("filterx fasta")
                    .bold()
                    .white(" command instead.");
            }
            h.print_and_exit();
        }

        // fill name and comment
        record._name.0 = 1;
        record._name.1 = record.buffer.len();

        // remove \n or \r\n
        let mut end = record.buffer.len();
        let name = &record.buffer[..];
        if name.ends_with(&[b'\n', b'\r']) {
            end -= 2;
        } else if name.ends_with(&[b'\n']) {
            end -= 1;
        }
        record.buffer.truncate(end);
        record._name.1 = end;

        if let Some(start) = record.buffer.iter().position(|&x| x == b' ') {
            record._name.1 = start;
            if self.parser_option.include_comment {
                record._comment.0 = start + 1;
                record._comment.1 = record.buffer.len();
            } else {
                record.buffer.truncate(start);
                record._comment.0 = 0;
                record._comment.1 = 0;
            }
        }

        // fill sequence
        record._sequence.0 = record.buffer.len();
        loop {
            line_buff.clear();
            let bytes = self.reader.read_until(b'\n', &mut line_buff)?;
            if bytes == 0 {
                return Err(crate::FilterxError::FastqError(
                    "Invalid fastq format: sequence".to_string(),
                ));
            }
            if line_buff[0] == b'+' {
                break;
            }
            // remove \n or \r\n
            let mut line = line_buff.as_slice();
            if line.ends_with(&[b'\r', b'\n']) {
                line = &line[..line.len() - 2];
            } else {
                line = &line[..line.len() - 1];
            }
            util::append_vec(&mut record.buffer, line);
        }
        record._sequence.1 = record.buffer.len();

        if self.parser_option.include_qual {
            record._qual.0 = record.buffer.len();
        }
        let mut nqual = 0;
        loop {
            line_buff.clear();
            let bytes = self.reader.read_until(b'\n', &mut line_buff)?;
            if bytes == 0 && nqual == 0 {
                return Err(crate::FilterxError::FastqError(
                    "Invalid fastq format: qual".to_string(),
                ));
            }
            if bytes == 0 {
                break;
            }
            nqual += 1;
            if line_buff[0] == b'@' {
                break;
            } else {
                if !self.parser_option.include_qual {
                    continue;
                }
                // remove \n or \r\n
                let mut line = line_buff.as_slice();
                if line.ends_with(&[b'\r', b'\n']) {
                    line = &line[..line.len() - 2];
                } else {
                    line = &line[..line.len() - 1];
                }
                util::append_vec(&mut record.buffer, line);
            }
        }

        if self.parser_option.include_qual {
            record._qual.1 = record.buffer.len();
        }
        return Ok(Some(record));
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
        cols.push(Column::new("name".into(), &names));
        cols.push(Column::new("seq".into(), &sequences));
        if !quals.is_empty() {
            cols.push(Column::new("qual".into(), &quals));
        }
        if !comments.is_empty() {
            cols.push(Column::new("comm".into(), &comments));
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
        cols.push(Column::new("name".into(), &names));
        if !comments.is_empty() {
            cols.push(Column::new("comm".into(), &comments));
        }
        cols.push(Column::new("seq".into(), &sequences));
        if !quals.is_empty() {
            cols.push(Column::new("qual".into(), &quals));
        }

        let df = DataFrame::new(cols)?;
        Ok(df)
    }

    fn reset(&mut self) -> FilterxResult<()> {
        self.reader.reset()?;
        self.read_end = false;
        Ok(())
    }
}
