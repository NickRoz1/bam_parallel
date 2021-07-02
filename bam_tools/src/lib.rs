mod block;
pub mod gz;
mod util;
mod virtual_position;
pub mod sort {
    mod comparators;
    mod flags;
    pub mod sort;
}
mod reader;

use block::Block;
pub use reader::Reader;
use std::mem;
use util::{fetch_block, inflate_data};
use virtual_position::VirtualPosition;

pub const MEGA_BYTE_SIZE: usize = 1024 * 1024;
pub(crate) const GIGA_BYTE_SIZE: usize = 1024 * MEGA_BYTE_SIZE;
const U64_SIZE: usize = mem::size_of::<u64>();
const U32_SIZE: usize = mem::size_of::<u32>();
const U16_SIZE: usize = mem::size_of::<u16>();
const U8_SIZE: usize = mem::size_of::<u8>();
const MAGIC_NUMBER: &[u8] = b"BAM\x01";
