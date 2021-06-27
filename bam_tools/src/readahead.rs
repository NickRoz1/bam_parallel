use crate::block::Block;
use crate::util::{fetch_block, inflate_data};

// This module preparses GBAM blocks to parallelize decompression
use super::util;
use flume::{Receiver, Sender};
use rayon::ThreadPool;
use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

enum Status {
    Success(Block),
    EOF,
}

struct ReaderGuard {
    last_access: usize,
    reader: Box<dyn Read + Send + 'static>,
}

impl ReaderGuard {
    pub fn get_task_num(&mut self) -> usize {
        let task_num = self.last_access;
        self.last_access += 1;
        task_num
    }

    pub fn get_inner(&mut self) -> &mut Box<dyn Read + Send + 'static> {
        &mut self.reader
    }
}
/// Prefetches and decompresses GBAM blocks
pub(crate) struct Readahead {
    // Reader which will source compressed data.
    inner: Arc<Mutex<ReaderGuard>>,
    // Decompressing threadpool.
    pool: ThreadPool,
    // Used to hold free buffers.
    read_bufs_recv: Receiver<Vec<u8>>,
    // Used to return buffer.
    read_bufs_send: Sender<Vec<u8>>,
    sender: Sender<Status>,
    receiver: Receiver<Status>,
    reached_eof: Arc<AtomicBool>,
    cvar_task_num: Arc<(Mutex<usize>, Condvar)>,
}

impl Readahead {
    pub fn new(thread_num: usize, reader: Box<dyn Read + Send + 'static>) -> Self {
        let (sender, receiver) = flume::unbounded();
        let (read_bufs_send, read_bufs_recv) = flume::unbounded();
        (0..thread_num).for_each(|_| read_bufs_send.send(Vec::new()).unwrap());
        Self {
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(thread_num)
                .build()
                .unwrap(),
            inner: Arc::new(Mutex::new(ReaderGuard {
                reader,
                last_access: 0,
            })),
            read_bufs_send,
            read_bufs_recv,
            sender,
            receiver,
            reached_eof: Arc::new(AtomicBool::new(false)),
            cvar_task_num: Arc::new((Mutex::new(0), Condvar::new())),
        }
    }

    /// Creates task to parse block at position of certain size
    pub fn prefetch_block(&mut self, mut block: Block) {
        let arc_reader_guard = self.inner.clone();
        let buf_sender = self.read_bufs_send.clone();
        let send_res = self.sender.clone();
        let cvar = self.cvar_task_num.clone();
        let mut read_buf = self.read_bufs_recv.recv().unwrap();
        let eof_reached_flag = Arc::clone(&self.reached_eof);

        self.pool.spawn(move || {
            if eof_reached_flag.load(Ordering::SeqCst) == true {
                return;
            }
            let mut reader_guard = arc_reader_guard.lock().unwrap();
            let cur_task_num = reader_guard.get_task_num();

            let bytes_count =
                fetch_block(reader_guard.get_inner(), &mut read_buf, &mut block).unwrap();

            if bytes_count == 0 {
                // Reached EOF.
                eof_reached_flag.swap(true, Ordering::SeqCst);
                let (lock, cvar) = &*cvar;

                let mut waiting_for_task_num = lock.lock().unwrap();

                while *waiting_for_task_num != cur_task_num {
                    waiting_for_task_num = cvar.wait(waiting_for_task_num).unwrap();
                }
                send_res.send(Status::EOF).unwrap();
                buf_sender.send(read_buf).unwrap();

                *waiting_for_task_num += 1;
                cvar.notify_all();
                return;
            }
            // Release mutex guard since reader is not needed anymore.
            drop(reader_guard);

            decompress_block(&read_buf[..], &mut block);

            // We don't need this buffer anymore, leave it for other threads.
            buf_sender.send(read_buf).unwrap();

            let (lock, cvar) = &*cvar;

            let mut waiting_for_task_num = lock.lock().unwrap();
            while *waiting_for_task_num != cur_task_num {
                waiting_for_task_num = cvar.wait(waiting_for_task_num).unwrap();
            }
            send_res.send(Status::Success(block)).unwrap();
            *waiting_for_task_num += 1;
            cvar.notify_all();
        });
    }

    /// Receives prefetched block. This is a blocking function. In case there is
    /// no uncompressed blocks in queue, the thread which called it will be
    /// blocked until uncompressed buffer appears.
    pub fn get_block(&mut self, old_buf: Block) -> Option<Block> {
        self.prefetch_block(old_buf);
        match self.receiver.recv().unwrap() {
            Status::Success(block) => Some(block),
            Status::EOF => None,
        }
    }
}

fn decompress_block(read_buf: &[u8], block: &mut Block) {
    let udata = block.data_mut();
    let udata_buf = udata.get_mut();
    inflate_data(&read_buf[..], udata_buf).expect("Decompression failed.");
    udata.set_position(0);
}
