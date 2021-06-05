mod block;
pub mod gz;
mod parallel_reader;
mod util;
mod virtual_position;
mod sort {
    mod sort;
}

use block::Block;
pub use parallel_reader::ParallelReader;
use util::{fetch_block, inflate_data};
use virtual_position::VirtualPosition;

pub(crate) const MEGA_BYTE_SIZE : usize = 1024*1024;
pub(crate) const GIGA_BYTE_SIZE : usize = 1024*MEGA_BYTE_SIZE;