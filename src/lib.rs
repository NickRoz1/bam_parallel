mod block;
pub mod gz;
mod parallel_reader;
mod util;
mod virtual_position;

use block::Block;
pub use parallel_reader::ParallelReader;
use util::{fetch_block, inflate_data};
use virtual_position::VirtualPosition;
