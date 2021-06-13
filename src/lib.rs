mod block;
pub mod gz;
mod parallel_reader;
mod util;
mod virtual_position;
mod sort {
    mod comparators;
    mod flags;
    mod sort;
}

use block::Block;
pub use parallel_reader::ParallelReader;
use std::mem;
use util::{fetch_block, inflate_data};
use virtual_position::VirtualPosition;

pub(crate) const MEGA_BYTE_SIZE: usize = 1024 * 1024;
pub(crate) const GIGA_BYTE_SIZE: usize = 1024 * MEGA_BYTE_SIZE;
const U64_SIZE: usize = mem::size_of::<u64>();
const U32_SIZE: usize = mem::size_of::<u32>();
const U16_SIZE: usize = mem::size_of::<u16>();
const U8_SIZE: usize = mem::size_of::<u8>();
