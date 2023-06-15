use crate::block::Block;
use crate::util::{fetch_block, inflate_data};

// This module preparses BAM blocks to parallelize decompression
use flume::{Receiver, Sender};
use std::io::Read;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

type VectorOfSendersAndReceivers = Vec<(Sender<Block>, Receiver<Block>)>;
/// Prefetches and decompresses GBAM blocks
pub(crate) struct Readahead {
    circular_buf_channels: VectorOfSendersAndReceivers,
    handles: Vec<Option<JoinHandle<()>>>,
    current_task: usize,
}

impl Readahead {
    pub fn new(mut thread_num: usize, reader: Box<dyn Read + Send + Sync>) -> Self {
        thread_num = std::cmp::max(thread_num, 1);

        let mut circular_buf_channels = VectorOfSendersAndReceivers::new();

        let mut handles: Vec<Option<JoinHandle<()>>> = Vec::new();

        // Due to I/O unpredictable nature, it may happen that two or more threads would race to lock a mutex on a reader
        // making uncompressed block stream unordered. Ensure order with condvar.
        let cond_var_for_order: Arc<(Mutex<usize>, Condvar)> =
            Arc::new((Mutex::new(0), Condvar::new()));

        let mutex_protected_reader = Arc::new(Mutex::new(reader));
        for i in 0..thread_num {
            let (block_sender, block_receiver) = flume::bounded(1);
            let (uncompressed_sender, uncompressed_receiver) = flume::bounded(1);
            let clone_of_reader = mutex_protected_reader.clone();
            let cond_var_for_this_thread = cond_var_for_order.clone();

            let thread = thread::spawn(move || {
                let mut read_buf = Vec::new();

                for mut block in block_receiver {
                    {
                        // Only one thread fetches data from file at a time.
                        let (lock, cvar) = &*cond_var_for_this_thread;
                        let mut my_turn = lock.lock().unwrap();

                        while *my_turn != i {
                            my_turn = cvar.wait(my_turn).unwrap();
                        }

                        let bytes_count = fetch_block(
                            clone_of_reader.lock().unwrap().as_mut(),
                            &mut read_buf,
                            &mut block,
                        )
                        .unwrap();

                        *my_turn += 1;
                        if *my_turn == thread_num {
                            *my_turn = 0;
                        }
                        cvar.notify_all();
                        if bytes_count == 0 {
                            // Reached EOF.
                            return;
                        }
                        // After this line the mutex lock will be dropped, and the decompression will happen in parallel to other threads.
                    }

                    decompress_block(&read_buf, &mut block);
                    uncompressed_sender.send(block).unwrap();
                }
            });
            block_sender.send(Block::default()).unwrap();
            handles.push(Some(thread));
            circular_buf_channels.push((block_sender, uncompressed_receiver));
        }
        Self {
            circular_buf_channels,
            handles,
            current_task: 0,
        }
    }

    /// Receives prefetched block. This is a blocking function. In case there is
    /// no uncompressed blocks in the queue, the thread which called it will be
    /// blocked until uncompressed buffer appears.
    pub fn get_block(&mut self, old_buf: Block) -> Option<Block> {
        if self.current_task == self.circular_buf_channels.len() {
            self.current_task = 0;
        }
        let cur_thread = &mut self.circular_buf_channels[self.current_task];
        let res = cur_thread.1.recv();
        // The thread reached EOF.
        if res.is_err() {
            // Join all of the other threads. The other threads should also reach the EOF by then.
            for j in self.handles.drain(0..) {
                j.unwrap().join().unwrap();
            }
            return None;
        }
        cur_thread.0.send(old_buf).unwrap();
        self.current_task += 1;
        return Some(res.unwrap());
    }
}

fn decompress_block(read_buf: &[u8], block: &mut Block) {
    let udata = block.data_mut();
    let udata_buf = udata.get_mut();
    inflate_data(read_buf, udata_buf).expect("Decompression failed.");
    udata.set_position(0);
}
