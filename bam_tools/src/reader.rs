mod readahead;
mod records;

use crate::block::Block;
use crate::MAGIC_NUMBER;
use byteorder::{LittleEndian, ReadBytesExt};
use std::ffi::CStr;
use std::io;

use readahead::Readahead;
use records::Records;
use std::convert::TryFrom;
use std::io::Read;

pub struct Reader {
    readahead: Readahead,
    block_buffer: Option<Block>,
    eof_reached: bool,
}

impl Reader {
    pub fn new<RSS: Read + Send + 'static>(inner: RSS, mut thread_num: usize) -> Self {
        if thread_num > num_cpus::get() {
            thread_num = num_cpus::get();
        }
        let readahead = Readahead::new(thread_num, Box::new(inner));
        Self {
            readahead,
            block_buffer: Some(Block::default()),
            eof_reached: false,
        }
    }

    pub fn read_record(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        buf.clear();
        self.append_record(buf)
    }

    // Resizes the buffer so an additional record can fit in the end and fills this empty section.
    pub fn append_record(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        let block_size = self.read_block_size();

        let prev_len = buf.len();
        buf.resize(prev_len + block_size, 0);

        self.read_exact(&mut buf[prev_len..prev_len + block_size])
            .expect("Failed to read.");

        Ok(block_size)
    }

    fn read_block_size(&mut self) -> usize {
        match self.read_u32::<LittleEndian>() {
            Ok(bs) => bs as usize,
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => 0,
            Err(e) => panic!("{}", e),
        }
    }

    pub fn records(&mut self) -> Records<'_> {
        Records::new(self)
    }

    pub fn read_header(&mut self) -> io::Result<String> {
        let magic = read_magic(self)?;

        if magic != MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid BAM header",
            ));
        }

        read_header(self)
    }

    pub fn parse_reference_sequences(&mut self) -> io::Result<Vec<(String, i32)>> {
        let n_ref = self.read_u32::<LittleEndian>().and_then(|n| {
            usize::try_from(n).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        })?;

        let mut vec = Vec::new();
        for _ in 0..n_ref {
            vec.push(parse_reference_sequence(self)?);
        }

        Ok(vec)
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.eof_reached {
            return Ok(0);
        }
        match self.block_buffer.as_mut().unwrap().data_mut().read(buf) {
            // Block exhausted, get new.
            Ok(0) => {
                // println!("Requested {} bytes", buf.len());
                match self.readahead.get_block(self.block_buffer.take().unwrap()) {
                    // EOF
                    None => {
                        self.eof_reached = true;
                        Ok(0)
                    }
                    // New block has been read. Continue reading.
                    Some(new_block) => {
                        self.block_buffer = Some(new_block);
                        // https://rust-lang.github.io/rfcs/0980-read-exact.html#about-errorkindinterrupted
                        Err(std::io::Error::from(io::ErrorKind::Interrupted))
                    }
                }
            }
            Ok(n) => Ok(n),
            Err(e) => Err(e),
        }
    }
}

fn read_magic<R>(reader: &mut R) -> io::Result<[u8; 4]>
where
    R: Read,
{
    let mut magic = [0; 4];
    reader.read_exact(&mut magic)?;
    Ok(magic)
}

fn read_header<R>(reader: &mut R) -> io::Result<String>
where
    R: Read,
{
    let l_text = reader.read_u32::<LittleEndian>().and_then(|n| {
        usize::try_from(n).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    })?;

    let mut text = vec![0; l_text];
    reader.read_exact(&mut text)?;

    // § 4.2 The BAM format (2021-06-03): "Plain header text in SAM; not necessarily
    // NUL-terminated".
    bytes_with_nul_to_string(&text).or_else(|_| {
        String::from_utf8(text).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    })
}

fn bytes_with_nul_to_string(buf: &[u8]) -> io::Result<String> {
    CStr::from_bytes_with_nul(buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        .and_then(|c_str| {
            c_str
                .to_str()
                .map(|s| s.to_string())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        })
}

fn parse_reference_sequence<R>(reader: &mut R) -> io::Result<(String, i32)>
where
    R: Read,
{
    let l_name = reader.read_u32::<LittleEndian>().and_then(|n| {
        usize::try_from(n).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    })?;

    let mut c_name = vec![0; l_name];
    reader.read_exact(&mut c_name)?;

    let _name = bytes_with_nul_to_string(&c_name)?;
    let _l_ref = reader.read_u32::<LittleEndian>().and_then(|len| {
        i32::try_from(len).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    })?;

    Ok((_name, _l_ref))
}
