use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::ops::{Deref, DerefMut};

use crate::error::FilterxResult;

pub struct TableLikeReaderInner<R> {
    _reader: R,
    _path: String,
}

impl Deref for TableLikeReaderInner<BufReader<std::fs::File>> {
    type Target = BufReader<std::fs::File>;

    fn deref(&self) -> &Self::Target {
        &self._reader
    }
}

impl Deref for TableLikeReaderInner<BufReader<GzDecoder<std::fs::File>>> {
    type Target = BufReader<GzDecoder<std::fs::File>>;

    fn deref(&self) -> &Self::Target {
        &self._reader
    }
}

impl DerefMut for TableLikeReaderInner<BufReader<std::fs::File>> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self._reader
    }
}

impl DerefMut for TableLikeReaderInner<BufReader<GzDecoder<std::fs::File>>> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self._reader
    }
}

pub enum TableLikeReader {
    PLAIN(TableLikeReaderInner<BufReader<std::fs::File>>),
    GZIP(TableLikeReaderInner<BufReader<GzDecoder<std::fs::File>>>),
}

impl TableLikeReader {
    pub fn new(path: &str) -> FilterxResult<Self> {
        // test if is gzip
        let mut gzip = false;
        let mut f = std::fs::File::open(path)?;
        let mut buff: [u8; 2] = [0; 2];
        let bytes = f.read(&mut buff[..])?;
        if bytes == 2 {
            if buff[0] == 0x1f && buff[1] == 0x8b {
                gzip = true;
            }
        }

        f.seek(SeekFrom::Start(0))?;

        if gzip {
            Ok(TableLikeReader::GZIP(TableLikeReaderInner {
                _reader: BufReader::new(GzDecoder::new(f)),
                _path: path.into(),
            }))
        } else {
            Ok(TableLikeReader::PLAIN(TableLikeReaderInner {
                _reader: BufReader::new(f),
                _path: path.into(),
            }))
        }
    }

    pub fn reset(&mut self) -> FilterxResult<()> {
        match self {
            Self::PLAIN(inner) => inner._reader.seek(SeekFrom::Start(0))?,
            Self::GZIP(inner) => {
                // TODO: more efficient way to reset the reader
                let f = File::open(&inner._path)?;
                inner._reader = BufReader::new(GzDecoder::new(f));
                0_u64
            }
        };
        Ok(())
    }
}

impl Clone for TableLikeReader {
    fn clone(&self) -> Self {
        match self {
            Self::PLAIN(inner) => TableLikeReader::new(&inner._path).unwrap(),
            Self::GZIP(inner) => TableLikeReader::new(&inner._path).unwrap(),
        }
    }
}

impl Read for TableLikeReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::PLAIN(inner) => inner._reader.read(buf),
            Self::GZIP(inner) => inner._reader.read(buf),
        }
    }
}

impl BufRead for TableLikeReader {
    fn consume(&mut self, amt: usize) {
        match self {
            Self::PLAIN(inner) => inner._reader.consume(amt),
            Self::GZIP(inner) => inner._reader.consume(amt),
        }
    }

    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        match self {
            Self::PLAIN(inner) => inner._reader.fill_buf(),
            Self::GZIP(inner) => inner._reader.fill_buf(),
        }
    }
}
