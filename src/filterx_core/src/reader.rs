use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

use memchr::memrchr;

use crate::FilterxResult;

pub enum FilterxReader {
    Plain(PlainReader),
    Gzip(GzipReader),
}

pub struct PlainReader {
    pub file: BufReader<std::fs::File>,
    pub path: String,
}

impl PlainReader {
    pub fn new(path: &str) -> FilterxResult<Self> {
        let file = std::fs::File::open(path)?;
        let file = BufReader::new(file);
        Ok(Self {
            file,
            path: path.to_string(),
        })
    }
}

pub struct GzipReader {
    pub gzip: BufReader<flate2::read::MultiGzDecoder<std::fs::File>>,
    pub path: String,
}

impl GzipReader {
    pub fn new(path: &str) -> FilterxResult<Self> {
        let fp = std::fs::File::open(path)?;
        let gzip = BufReader::new(flate2::read::MultiGzDecoder::new(fp));
        Ok(Self {
            gzip,
            path: path.to_string(),
        })
    }
}

#[derive(Debug, PartialEq, PartialOrd, clap::ValueEnum, Clone, Copy)]
pub enum FileContentType {
    Plain,
    Gzip,
    Auto,
}

impl FileContentType {
    pub fn from_path(path: &str) -> FilterxResult<Self> {
        let path_lower = path.to_lowercase();
        if path_lower.ends_with(".gz") || path_lower.ends_with(".gzip") {
            return Ok(FileContentType::Gzip);
        } else {
            return Ok(FileContentType::Plain);
        }
    }

    pub fn from_content(path: &str) -> FilterxResult<Self> {
        let mut file = std::fs::File::open(path)?;
        let mut buff = [0; 4];
        file.read_exact(&mut buff)?;
        match buff {
            [0x1f, 0x8b, _, _] => Ok(FileContentType::Gzip),
            _ => Ok(FileContentType::Plain),
        }
    }
}

impl FilterxReader {
    pub fn new(path: &str) -> FilterxResult<Self> {
        let file_type = FileContentType::from_content(path)?;
        let r;

        match file_type {
            FileContentType::Gzip => {
                r = FilterxReader::Gzip(GzipReader::new(path)?);
            }
            _ => {
                r = FilterxReader::Plain(PlainReader::new(path)?);
            }
        }

        Ok(r)
    }
}

impl Read for FilterxReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            FilterxReader::Plain(reader) => reader.file.read(buf),
            FilterxReader::Gzip(reader) => reader.gzip.read(buf),
        }
    }
}

impl BufRead for FilterxReader {
    fn consume(&mut self, amt: usize) {
        match self {
            FilterxReader::Plain(reader) => reader.file.consume(amt),
            FilterxReader::Gzip(reader) => reader.gzip.consume(amt),
        }
    }

    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        match self {
            FilterxReader::Plain(reader) => reader.file.fill_buf(),
            FilterxReader::Gzip(reader) => reader.gzip.fill_buf(),
        }
    }
}

impl FilterxReader {
    pub fn reset(&mut self) -> FilterxResult<()> {
        match self {
            FilterxReader::Plain(reader) => reader.file.seek(SeekFrom::Start(0))?,
            FilterxReader::Gzip(reader) => {
                let new_reader = GzipReader::new(&reader.path)?;
                reader.gzip = new_reader.gzip;
                0_u64
            }
        };
        Ok(())
    }

    pub fn path(&self) -> &str {
        match self {
            FilterxReader::Plain(reader) => reader.path.as_str(),
            FilterxReader::Gzip(reader) => reader.path.as_str(),
        }
    }
}

pub fn detect_breakline_len(reader: &mut FilterxReader) -> FilterxResult<Option<usize>> {
    let mut break_line_len = 0;
    loop {
        let data = reader.fill_buf()?;
        if data.is_empty() {
            break;
        }
        let offset = memrchr(b'\n', data);
        if offset.is_some() {
            // test if endwith is \r\n
            let offset = offset.unwrap();
            if offset > 0 && data[offset - 1] == b'\r' {
                break_line_len = 2;
            } else {
                break_line_len = 1;
            }
            break;
        }
    }
    reader.reset()?;
    Ok(Some(break_line_len))
}

impl Clone for FilterxReader {
    fn clone(&self) -> Self {
        FilterxReader::new(self.path()).unwrap()
    }
}
