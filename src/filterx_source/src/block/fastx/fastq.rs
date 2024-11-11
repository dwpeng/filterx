use polars::prelude::*;
use std::io::BufRead;

use crate::block::reader::TableLikeReader;
use crate::block::table_like::TableLike;

use filterx_core::{FilterxError, FilterxResult, Hint};

pub struct FastqSource {
    pub fastq: Fastq,
    pub records: Vec<FastqRecord>,
}

impl Drop for FastqSource {
    fn drop(&mut self) {
        unsafe {
            self.records.set_len(self.records.capacity());
        }
    }
}

impl FastqSource {
    pub fn new(path: &str, include_comment: bool, include_qual: bool) -> FilterxResult<Self> {
        let parser_option = FastqParserOption {
            include_comment,
            include_qual,
        };
        let fastq = Fastq::from_path(path)?.set_parser_options(parser_option);
        let records = vec![FastqRecord::default(); 4096];
        Ok(FastqSource { fastq, records })
    }

    pub fn into_dataframe(&mut self, n: usize) -> FilterxResult<Option<DataFrame>> {
        let records = &mut self.records;

        if records.capacity() < n {
            unsafe {
                records.set_len(records.capacity());
            }
            for _ in records.capacity()..=n {
                records.push(FastqRecord::default());
            }
        }
        unsafe {
            records.set_len(n);
        }
        let mut count = 0;
        while let Some(record) = self.fastq.parse_next()? {
            let r = unsafe { records.get_unchecked_mut(count) };
            r.clear();
            r.buffer.extend_from_slice(&record.buffer);
            r._name = record._name;
            r._comment = record._comment;
            r._sequence = record._sequence;
            r._qual = record._qual;
            count += 1;
            if count >= n {
                break;
            }
        }
        unsafe {
            records.set_len(count);
        }
        if records.is_empty() {
            Ok(None)
        } else {
            let df = Fastq::as_dataframe(&records, &self.fastq.parser_option)?;
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
    break_line_len: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct FastqRecord {
    buffer: Vec<u8>,
    _name: (usize, usize),
    _sequence: (usize, usize),
    _qual: (usize, usize),
    _comment: (usize, usize),
}

impl Default for FastqRecord {
    fn default() -> Self {
        FastqRecord {
            buffer: Vec::with_capacity(256),
            _name: (0, 0),
            _sequence: (0, 0),
            _qual: (0, 0),
            _comment: (0, 0),
        }
    }
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
    pub fn format<'a>(&'a self) -> &'a str {
        unsafe { std::str::from_utf8_unchecked(&self.buffer) }
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

impl TableLike for Fastq {
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
            break_line_len: None,
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
    fn parse_next(&mut self) -> FilterxResult<Option<&mut Self::Record>> {
        if self.read_end {
            return Ok(None);
        }
        let record = &mut self.record;
        let mut line_buff = &mut self.line_buffer;
        record.clear();
        if !line_buff.is_empty() {
            record.buffer.extend_from_slice(line_buff);
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
        let end = record.buffer.len();
        let break_line_len;
        if self.break_line_len.is_some() {
            break_line_len = self.break_line_len.unwrap();
        } else {
            if record.buffer.ends_with(&[b'\r', b'\n']) {
                break_line_len = 2;
            } else {
                break_line_len = 1;
            }
            self.break_line_len = Some(break_line_len);
        }
        record._name.1 = end - break_line_len;

        if let Some(start) = record.buffer.iter().position(|&x| x == b' ') {
            record._name.1 = start;
            if self.parser_option.include_comment {
                record._comment.0 = start + 1;
                record._comment.1 = record.buffer.len();
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
            line_buff.clear();
            let bytes = self.reader.read_until(b'\n', &mut line_buff)?;
            if bytes == 0 {
                return Err(FilterxError::FastqError(
                    "Invalid fastq format: sequence".to_string(),
                ));
            }
            if line_buff[0] == b'+' {
                if self.parser_option.include_qual {
                    record.buffer.extend_from_slice(&[b'\n', b'+', b'\n']);
                }
                break;
            }
            let line = &line_buff[..bytes - break_line_len];
            record.buffer.extend_from_slice(line);
        }
        if self.parser_option.include_qual{
            record._sequence.1 = record.buffer.len() - 3;
        }else{
            record._sequence.1 = record.buffer.len();
        }

        if self.parser_option.include_qual {
            record._qual.0 = record.buffer.len();
        }
        let mut nqual = 0;
        loop {
            line_buff.clear();
            let bytes = self.reader.read_until(b'\n', &mut line_buff)?;
            if bytes == 0 && nqual == 0 {
                return Err(FilterxError::FastqError(
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
                let line = &line_buff[..bytes - break_line_len];
                record.buffer.extend_from_slice(line);
            }
        }

        if self.parser_option.include_qual {
            record._qual.1 = record.buffer.len();
        } else {
            record.buffer[0] = b'>';
        }
        return Ok(Some(record));
    }

    fn set_parser_options(self, parser_options: Self::ParserOptions) -> Self {
        let mut fastq = self;
        fastq.parser_option = parser_options;
        fastq
    }

    fn as_dataframe(
        records: &Vec<Self::Record>,
        parser_options: &Self::ParserOptions,
    ) -> FilterxResult<DataFrame> {
        let mut names: Vec<&str> = Vec::with_capacity(records.len());
        let mut sequences: Vec<&str> = Vec::with_capacity(records.len());
        let mut quals: Vec<&str> = if parser_options.include_qual {
            Vec::with_capacity(records.len())
        } else {
            Vec::with_capacity(0)
        };
        let mut comments: Vec<&str> = if parser_options.include_comment {
            Vec::with_capacity(records.len())
        } else {
            Vec::with_capacity(0)
        };
        for record in records {
            names.push(record.name());
            sequences.push(record.seq());
            if let Some(qual) = record.qual() {
                quals.push(qual);
            } else {
                if parser_options.include_qual {
                    quals.push("");
                }
            }
            if let Some(comment) = record.comment() {
                comments.push(comment);
            } else {
                if parser_options.include_comment {
                    comments.push("");
                }
            }
        }

        let mut cols = Vec::with_capacity(4);
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
