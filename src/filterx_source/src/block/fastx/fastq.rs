use polars::prelude::*;
use std::{fmt::Display, io::BufRead};

use crate::dataframe::DataframeSource;

use filterx_core::{
    reader::{detect_breakline_len, FilterxReader},
    FilterxError, FilterxResult, Hint,
};

pub struct FastqSource {
    pub fastq: Fastq,
    pub records: Vec<FastqRecord>,
    pub dataframe: DataframeSource,
}

impl Drop for FastqSource {
    fn drop(&mut self) {
        unsafe {
            self.records.set_len(self.records.capacity());
        }
    }
}

impl FastqSource {
    pub fn new(
        path: &str,
        include_comment: bool,
        include_qual: bool,
        quality_type: QualityType,
        detect_size: usize,
    ) -> FilterxResult<Self> {
        let parser_option = FastqParserOption {
            include_comment,
            include_qual,
            phred: quality_type,
        };
        let fastq =
            Fastq::from_path(path, quality_type, detect_size)?.set_parser_options(parser_option);
        let records = vec![FastqRecord::default(); 4096];
        let dataframe = DataframeSource::new(DataFrame::empty().lazy());
        Ok(FastqSource {
            fastq,
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
            Ok(0)
        } else {
            let df = Fastq::as_dataframe(&records, &self.fastq.parser_option)?;
            self.dataframe.update(df.lazy());
            Ok(count)
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
    pub phred: QualityType,
}

impl Default for FastqParserOption {
    fn default() -> Self {
        FastqParserOption {
            include_qual: true,
            include_comment: true,
            phred: QualityType::Phred33,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum QualityType {
    Phred33,
    Phred64,
    Auto,
}

impl Display for QualityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QualityType::Phred33 => write!(f, "phred33"),
            QualityType::Phred64 => write!(f, "phred64"),
            QualityType::Auto => write!(f, "unknown"),
        }
    }
}

impl Default for QualityType {
    fn default() -> Self {
        QualityType::Phred33
    }
}

pub struct Fastq {
    reader: FilterxReader,
    read_end: bool,
    pub path: String,
    pub parser_option: FastqParserOption,
    record: FastqRecord,
    break_line_len: Option<usize>,
    pub quality_type: QualityType,
    pub buffer_unprocess_size: usize,
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
        self._qual = (0, 0);
    }

    #[inline(always)]
    pub fn format<'a>(&'a self) -> &'a str {
        unsafe { std::str::from_utf8_unchecked(&self.buffer) }
    }

    pub fn as_phred33(&mut self) -> () {
        if self._qual.0 != self._qual.1 {
            for i in self._qual.0..self._qual.1 {
                self.buffer[i] = self.buffer[i] - 33;
            }
        }
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

    #[inline(always)]
    pub fn remove_breakline_from_buffer(&mut self, len: usize) {
        if len > 0 {
            unsafe {
                self.buffer.set_len(self.buffer.len() - len);
            }
        }
    }
}

impl Fastq {
    pub fn from_path(
        path: &str,
        quality_type: QualityType,
        detect_size: usize,
    ) -> FilterxResult<Fastq> {
        let mut fq = Fastq {
            reader: FilterxReader::new(path)?,
            read_end: false,
            parser_option: FastqParserOption::default(),
            path: path.to_string(),
            record: FastqRecord::default(),
            break_line_len: None,
            quality_type,
            buffer_unprocess_size: 0,
        };
        fq.break_line_len = detect_breakline_len(&mut fq.reader)?;
        if quality_type == QualityType::Auto {
            fq.guess_quality_type(detect_size)?;
        }
        Ok(fq)
    }

    fn guess_quality_type(&mut self, detect_size: usize) -> FilterxResult<()> {
        if !self.parser_option.include_qual {
            return Ok(());
        }
        let mut qualitys = vec![QualityType::Auto; detect_size];
        let mut count = 0;
        for _ in 0..detect_size {
            let record = self.parse_next()?;
            if let Some(record) = record {
                let qual = record.qual();
                if let Some(qual) = qual {
                    let qual_u8 = qual.as_bytes();
                    let max = qual_u8.iter().max().unwrap();
                    let min = qual_u8.iter().min().unwrap();
                    // Sanger:         0 - 40   +33   33 - 73   phred33
                    // Solexa:        -5 - 40   +64   59 - 124  phred64  not supported!
                    // Illumina 1.3:   0 - 40   +64   64 - 124  phred64
                    // Illumina 1.5:   3 - 40   +64   67 - 104  phred64  0,1,2 are clipped
                    // Illumina 1.8+:  0 - 41   +33   33 - 73   phred33
                    let new_quality_type = if *max >= 73 && *min >= 64 {
                        QualityType::Phred64
                    } else if *max <= 73 && *min >= 33 {
                        QualityType::Phred33
                    } else {
                        QualityType::Auto
                    };
                    qualitys[count] = new_quality_type;
                    count += 1;
                }
            }
        }
        let t = if count == 0 {
            QualityType::Auto
        } else {
            let mut t = qualitys[0];
            for i in 1..count {
                if qualitys[i] != t {
                    t = QualityType::Auto;
                    if t == QualityType::Auto {
                        return Err(filterx_core::FilterxError::FastqError(
                            "Fastq quality type is not consistent".to_string(),
                        ));
                    }
                    break;
                }
            }
            t
        };
        self.quality_type = t;
        self.reset()?;
        Ok(())
    }

    pub fn goto_next_record(&mut self) {
        self.record.goto_next_record(self.buffer_unprocess_size);
        self.buffer_unprocess_size = 0;
    }

    /// parse fastq format based paper: https://academic.oup.com/nar/article/38/6/1767/3112533
    pub fn parse_next(&mut self) -> FilterxResult<Option<&mut FastqRecord>> {
        if self.read_end {
            return Ok(None);
        }
        self.goto_next_record();
        let record: &mut FastqRecord = &mut self.record;

        if record.buffer.is_empty() {
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

        let break_line_len = self.break_line_len.unwrap();

        // fill name and comment
        record._name.0 = 1;
        record._name.1 = record.buffer.len() - break_line_len;

        let start = memchr::memchr(b' ', &record.buffer[1..record._name.1]);
        if let Some(mut start) = start {
            start += 1;
            record._name.1 = start;
            if self.parser_option.include_comment {
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
                return Err(FilterxError::FastqError(
                    "Invalid fastq format: sequence".to_string(),
                ));
            }
            if record.buffer[buffer_offset] == b'+' {
                unsafe {
                    record
                        .buffer
                        .set_len(record.buffer.len() - break_line_len - 1);
                }
                record._sequence.1 = record.buffer.len();
                if self.parser_option.include_qual {
                    record.buffer.extend_from_slice(&[b'\n', b'+', b'\n']);
                    record._qual.0 = record.buffer.len();
                }
                break;
            }
            record.remove_breakline_from_buffer(break_line_len);
        }

        let mut nqual = 0;

        loop {
            let buffer_offset = record.buffer.len();
            let bytes = self.reader.read_until(b'\n', &mut record.buffer)?;
            if bytes == 0 && nqual == 0 {
                return Err(FilterxError::FastqError(
                    "Invalid fastq format: qual".to_string(),
                ));
            }
            if bytes == 0 {
                self.read_end = true;
                if self.parser_option.include_qual {
                    record._qual.1 = record.buffer.len();
                } else {
                    record._qual = (0, 0);
                    unsafe {
                        record.buffer.set_len(record._sequence.1);
                    }
                }
                break;
            }
            nqual += bytes - break_line_len;
            if record.buffer[buffer_offset] == b'@' {
                if self.parser_option.include_qual {
                    unsafe {
                        record.buffer.set_len(buffer_offset);
                    }
                    record._qual.1 = buffer_offset;
                } else {
                    unsafe {
                        std::ptr::copy(
                            record.buffer.as_ptr().add(record.buffer.len() - bytes),
                            record.buffer.as_mut_ptr().add(record._sequence.1),
                            bytes,
                        );
                        record.buffer.set_len(record._sequence.1);
                    }
                }
                self.buffer_unprocess_size = bytes;
                break;
            } else {
                if !self.parser_option.include_qual {
                    continue;
                }
                record.remove_breakline_from_buffer(break_line_len);
            }
        }

        if self.parser_option.include_qual {
            record._qual.1 = record.buffer.len();
        } else {
            record.buffer[0] = b'>';
        }

        return Ok(Some(record));
    }

    pub fn set_parser_options(self, parser_options: FastqParserOption) -> Self {
        let mut fastq = self;
        fastq.parser_option = parser_options;
        fastq
    }

    pub fn as_dataframe(
        records: &Vec<FastqRecord>,
        parser_options: &FastqParserOption,
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

    pub fn reset(&mut self) -> FilterxResult<()> {
        self.reader.reset()?;
        self.read_end = false;
        self.record.buffer.clear();
        self.buffer_unprocess_size = 0;
        Ok(())
    }
}
