use std::io::{BufWriter, Write};

use gzp::{
    deflate::Gzip,
    par::compress::{ParCompress, ParCompressBuilder},
    Compression,
};

use crate::{reader::FileContentType, thread_size::ThreadSize, FilterxResult};

pub enum FilterxWriter {
    Stdout(StdoutWriter),
    Plain(PlainWriter),
    Gzip(GzipWriter),
}

pub struct StdoutWriter {
    pub stdout: BufWriter<std::io::Stdout>,
}

impl StdoutWriter {
    pub fn new() -> Self {
        let stdout = std::io::stdout();
        let stdout = BufWriter::new(stdout);
        Self { stdout }
    }
}

pub struct PlainWriter {
    pub file: BufWriter<std::fs::File>,
    pub path: String,
}

impl PlainWriter {
    pub fn new(path: &str) -> FilterxResult<Self> {
        let file = std::fs::File::create(path)?;
        let file = BufWriter::new(file);
        Ok(Self {
            file,
            path: path.to_string(),
        })
    }
}

pub struct GzipWriter {
    pub gzip: BufWriter<gzp::par::compress::ParCompress<gzp::deflate::Gzip>>,
    pub path: String,
    pub compression_level: u32,
    pub threads: usize,
}

impl GzipWriter {
    pub fn new(path: &str, compression_level: u32, threads: usize) -> FilterxResult<Self> {
        let fp = std::fs::File::create(path)?;
        let gzip_writer: ParCompress<Gzip> = ParCompressBuilder::new()
            .compression_level(Compression::new(compression_level))
            .num_threads(threads)?
            .from_writer(fp);

        let gzip_writer = BufWriter::new(gzip_writer);

        Ok(Self {
            gzip: gzip_writer,
            path: path.to_string(),
            compression_level,
            threads,
        })
    }
}

impl FilterxWriter {
    pub fn new(
        path: Option<String>,
        compression_level: Option<u32>,
        file_type: Option<FileContentType>,
    ) -> FilterxResult<Self> {
        let w;
        if path.is_none() {
            w = FilterxWriter::Stdout(StdoutWriter::new());
            return Ok(w);
        }

        let compression_level = match compression_level {
            Some(level) => level,
            None => 6,
        };

        let threads = ThreadSize::get();

        let path = path.unwrap();

        let file_type = match file_type {
            Some(FileContentType::Auto) => FileContentType::from_path(&path)?,
            Some(file_type) => file_type,
            None => FileContentType::from_path(&path)?,
        };

        match file_type {
            FileContentType::Gzip => {
                w = FilterxWriter::Gzip(GzipWriter::new(&path, compression_level, threads)?);
            }
            FileContentType::Plain => {
                w = FilterxWriter::Plain(PlainWriter::new(&path)?);
            }
            _ => unreachable!(),
        }
        Ok(w)
    }
}

impl Write for FilterxWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            FilterxWriter::Stdout(writer) => writer.stdout.write(buf),
            FilterxWriter::Plain(writer) => writer.file.write(buf),
            FilterxWriter::Gzip(writer) => writer.gzip.write(buf),
        }
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        match self {
            FilterxWriter::Stdout(writer) => writer.stdout.flush()?,
            FilterxWriter::Plain(writer) => writer.file.flush()?,
            FilterxWriter::Gzip(writer) => writer.gzip.flush()?,
        }
        Ok(())
    }
}
