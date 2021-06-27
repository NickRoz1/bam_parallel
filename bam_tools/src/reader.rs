use flate2::read;

use crate::block::Block;
use std::io;

use super::readahead::Readahead;
use std::{io::Read, os::unix::thread};

pub struct Reader {
    readahead: Readahead,
    block_buffer: Option<Block>,
}

impl Reader {
    pub fn new<RSS: Read + Send + 'static>(inner: RSS, thread_num: usize) -> Self {
        assert!(thread_num > 0 && thread_num < num_cpus::get());
        let mut readahead = Readahead::new(thread_num, Box::new(inner));
        for _ in 0..thread_num {
            readahead.prefetch_block(Block::default());
        }
        Self {
            readahead,
            block_buffer: Some(Block::default()),
        }
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let p = self.block_buffer.as_mut().unwrap();
        // println!("Current bgzf block size = {}", p.data_mut().get_ref().len());
        // Attempt to fill buf from current block.
        // println!("Requested {} bytes", buf.len());
        if buf.len() == 14 {
            // println!("Got something weird");
        }
        match self.block_buffer.as_mut().unwrap().data_mut().read(buf) {
            // Block exhausted, get new.
            Ok(0) => {
                // println!("Requested {} bytes", buf.len());
                match self.readahead.get_block(self.block_buffer.take().unwrap()) {
                    // EOF
                    None => {
                        println!("Reached eof?");
                        Ok(0)
                    }
                    // New block has been read. Continue reading.
                    Some(new_block) => {
                        if new_block.get_len() == 28 {
                            println!("EOF BLOCK");
                        }
                        self.block_buffer = Some(new_block);
                        // println!(
                        //     "Block size: {}",
                        //     self.block_buffer.as_ref().unwrap().get_len()
                        // );
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
