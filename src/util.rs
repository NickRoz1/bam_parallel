// Source: https://github.com/zaeleus/noodles/blob/master/noodles-bgzf/src/reader.rs

use super::gz::{BGZF_HEADER_SIZE, TRAILER_SIZE};
use super::Block;
use byteorder::{ByteOrder, LittleEndian};
use flate2::read::DeflateDecoder;
use std::io;
use std::io::Read;

pub(crate) fn inflate_data<R>(reader: R, writer: &mut Vec<u8>) -> io::Result<usize>
where
  R: Read,
{
  let mut decoder = DeflateDecoder::new(reader);
  decoder.read_to_end(writer)
}

pub(crate) fn fetch_block<R>(
  reader: &mut R,
  cdata: &mut Vec<u8>,
  block: &mut Block,
) -> io::Result<usize>
where
  R: Read,
{
  let block_size = match read_block_size(reader).map(usize::from) {
    Ok(0) => return Ok(0),
    Ok(bs) => bs,
    Err(e) => return Err(e),
  };

  let cdata_len = block_size - BGZF_HEADER_SIZE - TRAILER_SIZE;
  cdata.resize(cdata_len, Default::default());
  reader.read_exact(cdata)?;

  read_trailer(reader)?;

  block.set_len(block_size as u64);

  let udata = block.data_mut();
  let udata_buf = udata.get_mut();
  udata_buf.clear();

  Ok(block_size)
}

fn read_block_size<R>(reader: &mut R) -> io::Result<u16>
where
  R: Read,
{
  let mut header = [0; BGZF_HEADER_SIZE];

  if reader.read_exact(&mut header).is_err() {
    return Ok(0);
  }

  let bsize = &header[16..18];

  // Add 1 because BSIZE is "total Block SIZE minus 1".
  Ok(LittleEndian::read_u16(bsize) + 1)
}

fn read_trailer<R>(reader: &mut R) -> io::Result<()>
where
  R: Read,
{
  let mut trailer = [0; TRAILER_SIZE];
  reader.read_exact(&mut trailer)
}
