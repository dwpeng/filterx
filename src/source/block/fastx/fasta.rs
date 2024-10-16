use polars::prelude::*;
use std::io::BufRead;

use crate::source::block::reader::TableLikeReader;

use crate::source::block::record::Filter;
use crate::source::block::table_like::TableLike;

use super::FastxRecord;

use crate::error::FilterxResult;

#[derive(Debug, Copy, Clone, Default)]
pub struct FastaRcordFilterOptions {
    pub max_length: Option<usize>,
    pub min_length: Option<usize>,
    pub max_gc: Option<f64>,
    pub min_gc: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct FastaRecordParserOptions {
    pub comment_prefix: Option<Vec<String>>,
    pub uppercase: Option<bool>,
    pub lowercase: Option<bool>,
    pub rev_complement: Option<bool>,
    pub do_filter: bool,
}

pub struct FastaSource {
    pub fasta: Fasta,
    pub filter_options: Option<FastaRcordFilterOptions>,
}

impl FastaSource {
    pub fn new(path: &str) -> FilterxResult<Self> {
        Ok(FastaSource {
            fasta: Fasta::from_path(path)?,
            filter_options: None,
        })
    }

    pub fn set_filter_options(mut self, filter_options: FastaRcordFilterOptions) -> Self {
        self.filter_options = Some(filter_options);
        self
    }
}

pub struct Fasta {
    reader: TableLikeReader,
    prev_line: String,
    read_end: bool,
    pub path: String,
    pub filter_options: Option<FastaRcordFilterOptions>,
    pub parser_options: Option<FastaRecordParserOptions>,
    columns: Vec<String>,
    record: FastaRecord,
    pub record_type: FastaRecordType,
}

#[derive(Clone, Debug)]
pub enum FastaRecordType {
    DNA,
    RNA,
    PROTEIN,
}

#[derive(Clone, Debug, Default)]
pub struct FastaRecord {
    buffer: String,
    _name: (usize, usize),
    _comment: (usize, usize),
    _sequence: (usize, usize),
}

impl FastaRecord {
    pub fn new(raw: &str) -> Self {
        FastaRecord {
            buffer: raw.to_string(),
            _name: (0, 0),
            _comment: (0, 0),
            _sequence: (0, 0),
        }
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self._name = (0, 0);
        self._comment = (0, 0);
        self._sequence = (0, 0);
    }
}

impl FastxRecord for FastaRecord {
    fn name(&self) -> &str {
        &self.buffer[self._name.0..self._name.1]
    }

    fn comment(&self) -> Option<&str> {
        if self._comment.0 == self._comment.1 {
            None
        } else {
            Some(&self.buffer[self._comment.0..self._comment.1])
        }
    }

    fn qual(&self) -> Option<&str> {
        None
    }

    fn seq(&self) -> &str {
        &self.buffer[self._sequence.0..self._sequence.1]
    }

    unsafe fn mut_seq(&mut self) -> &mut str {
        &mut self.buffer[self._sequence.0..self._sequence.1]
    }
}

impl std::fmt::Display for FastaRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            ">{} {}\n{}",
            self.name(),
            self.comment().unwrap_or(""),
            self.seq()
        )
    }
}

impl Filter for FastaRecord {
    type FilterOptions = FastaRcordFilterOptions;

    fn filter(&self, filter_option: &Self::FilterOptions) -> bool {
        if let Some(max_length) = filter_option.max_length {
            if self.len() > max_length {
                return false;
            }
        }
        if let Some(min_length) = filter_option.min_length {
            if self.len() < min_length {
                return false;
            }
        }

        let mut gc: f64 = 0.0;
        let mut computed = false;

        if let Some(max) = filter_option.max_gc {
            let gc_count = self.gc();
            gc = gc_count as f64 / self.len() as f64;

            if gc > max {
                return false;
            }

            computed = true;
        }

        if let Some(min) = filter_option.min_gc {
            if !computed {
                let gc_count = self.gc();
                gc = gc_count as f64 / self.len() as f64;
            }

            if gc < min {
                return false;
            }
        }
        true
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

#[derive(PartialEq, Clone, Debug)]
enum FastaRecordEnum {
    Init,
    Header,
    Sequence,
}

impl Clone for Fasta {
    fn clone(&self) -> Self {
        Fasta {
            reader: self.reader.clone(),
            path: self.path.clone(),
            filter_options: self.filter_options.clone(),
            parser_options: self.parser_options.clone(),
            prev_line: String::new(),
            read_end: false,
            columns: vec!["name".to_string(), "seq".to_string()],
            record: self.record.clone(),
            record_type: self.record_type.clone(),
        }
    }
}

impl<'a> TableLike<'a> for Fasta {
    type Table = Fasta;
    type Record = FastaRecord;
    type FilterOptions = FastaRcordFilterOptions;
    type ParserOptions = FastaRecordParserOptions;

    fn from_path(path: &str) -> crate::error::FilterxResult<Self::Table> {
        Ok(Fasta {
            reader: TableLikeReader::new(path)?,
            prev_line: String::new(),
            read_end: false,
            path: path.to_string(),
            filter_options: None,
            parser_options: None,
            columns: vec!["name".to_string(), "seq".to_string()],
            record: FastaRecord::default(),
            record_type: FastaRecordType::DNA,
        })
    }

    fn set_filter_options(mut self, filter_options: Self::FilterOptions) -> Self {
        self.filter_options = Some(filter_options);
        self
    }

    fn set_parser_options(mut self, parser_options: Self::ParserOptions) -> Self {
        self.parser_options = Some(parser_options);
        self
    }

    fn reset(&mut self) {
        self.reader = TableLikeReader::new(&self.path).unwrap();
        self.prev_line.clear();
        self.read_end = false;
    }

    fn filter_next(&'a mut self) -> FilterxResult<Option<&'a Self::Record>> {
        let filter_options = self.filter_options;
        let record = self.parse_next()?;
        if let Some(r) = record {
            if let Some(o) = filter_options {
                if r.filter(&o) {
                    return Ok(record);
                }
            }
        }
        Ok(None)
    }

    fn parse_next(&'a mut self) -> FilterxResult<Option<&'a FastaRecord>> {
        let record: &mut FastaRecord = &mut self.record;
        'next_record: loop {
            if self.read_end {
                return Ok(None);
            }
            record.clear();
            let mut status = FastaRecordEnum::Init;
            loop {
                if self.prev_line.len() == 0 {
                    let bytes = self.reader.read_line(&mut self.prev_line)?;
                    if bytes == 0 {
                        self.read_end = true;
                        break;
                    }
                }
                let line = self.prev_line.trim_end();

                match status {
                    FastaRecordEnum::Init => {
                        assert!(record.buffer.is_empty());
                        if line.starts_with('>') {
                            if line.len() == 1 {
                                return Err(crate::error::FilterxError::FastaError(
                                    "Fasta record must have header".to_string(),
                                ));
                            }
                            record._name.0 = 1;
                            record.buffer.push_str(line);
                            record._name.1 = line.len();
                            status = FastaRecordEnum::Header;
                            let comment_start = line.find(' ');
                            if let Some(start) = comment_start {
                                record._comment.0 = start + 1;
                                record._comment.1 = record.buffer.len();
                                record._name.1 = start;
                            }
                            record.buffer.push_str(line);
                        } else {
                            return Err(crate::error::FilterxError::FastaError(
                                "Fasta record must start with '>'".to_string(),
                            ));
                        }
                    }
                    FastaRecordEnum::Header => {
                        if record._sequence.0 == 0 {
                            record._sequence.0 = record.buffer.len();
                        }
                        record.buffer.push_str(line);
                        status = FastaRecordEnum::Sequence;
                    }
                    FastaRecordEnum::Sequence => {
                        if line.starts_with('>') {
                            break;
                        } else {
                            record.buffer.push_str(line);
                        }
                    }
                }
                self.prev_line.clear();
            }

            record._sequence.1 = record.buffer.len();

            if status != FastaRecordEnum::Sequence || record.len() == 0 {
                return Err(crate::error::FilterxError::FastaError(
                    "Fasta record must have sequence".to_string(),
                ));
            }

            if let Some(p) = &self.parser_options {
                if p.do_filter {
                    if let Some(filter_options) = &self.filter_options {
                        if !record.filter(filter_options) {
                            continue 'next_record;
                        }
                    }
                }
            }
            return Ok(Some(record));
        }
    }

    fn into_dataframe(self) -> crate::error::FilterxResult<polars::prelude::DataFrame> {
        let mut headers: Vec<String> = Vec::new();
        let mut sequences: Vec<String> = Vec::new();
        let mut fasta = self;

        loop {
            match fasta.parse_next()? {
                Some(record) => {
                    headers.push(record.name().into());
                    sequences.push(record.seq().into());
                }
                None => break,
            }
        }

        let df = polars::prelude::DataFrame::new(vec![
            polars::prelude::Series::new("name".into(), headers),
            polars::prelude::Series::new("seq".into(), sequences),
        ])?;

        Ok(df)
    }

    fn as_dataframe(
        records: Vec<FastaRecord>,
    ) -> crate::error::FilterxResult<polars::prelude::DataFrame> {
        let mut headers: Vec<String> = Vec::new();
        let mut sequences: Vec<String> = Vec::new();
        for record in records {
            headers.push(record.name().into());
            sequences.push(record.seq().into());
        }
        let df = polars::prelude::DataFrame::new(vec![
            polars::prelude::Series::new("name".into(), headers),
            polars::prelude::Series::new("seq".into(), sequences),
        ])?;
        Ok(df)
    }

    fn columns(&self) -> &Vec<String> {
        &self.columns
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

    #[test]
    fn test_column_names() -> FilterxResult<()> {
        let path = "test_data/fasta/1.fa";
        let fasta = Fasta::from_path(path)?;
        let cols = fasta.columns();
        assert_eq!(cols, &["name", "seq"]);
        assert_eq!(cols[0], "name");
        assert_eq!(cols[1], "seq");
        Ok(())
    }
}
