use rayon::{self, ThreadBuilder, ThreadPoolBuilder};
use crate::{MEGA_BYTE_SIZE, GIGA_BYTE_SIZE};
use flume::{Sender, Receiver};
use std::cmp::{max, min};
use tempdir::TempDir;
use std::fs::File;

/// This struct manages buffer for unsorted reads
struct RecordsBuffer {
    mem_limit: usize,
    records: Vec<RawRecord>,
}

impl RecordsBuffer {
    pub fn new(mem_limit: usize) -> Self {
        RecordsBuffer {
            mem_limit,
        }
    }

    pub fn clear(&mut self) {
        self.records.clear();
    }

    pub fn fill<R>(&mut self, reader: R) {
        let mut mem_consumption: usize = 0;
        while let Some(rec) = reader.next() {
            mem_consumption += rec.byte_size();
            self.records.push(rec);
            // Buffer is filled
            if mem_consumption > self.mem_limit {
                break;
            }
        }
    }
}


struct Sorter<R, W> {
    reader: R,
    sorted_sink: W,
    thread_pool: rayon::ThreadPool,
    mem_limit: usize,
    out_compr_level: usize,
    buf_send: Sender<RecordsBuffer>,
    buf_recv: Receiver<RecordsBuffer>,
    tmp_dir: TempDir,
    tmp_file_counter: usize,
}

impl<R, W> Sorter<R,W> {
    /// There is no try_reserve container functionality in Rust yet, that means
    /// that if the allocation will fail the program using sort functionality
    /// will also crash with panic!. The memory limit should be chosen.
    /// carefully.
    pub fn new(mem_limit: usize, reader: R, sorted_sink: W, tmp_dir_path: std::path::PathBuf, out_compr_level: usize, thread_num: usize) -> Self {
        let (buf_send, buf_recv) = flume::unbounded();
        thread_num = max(min(num_cpus::get(), thread_num), 1);
        let thread_pool = ThreadPoolBuilder::new().num_threads(thread_num).build().unwrap();
        let tmp_dir = TempDir::new_in(tmp_dir_path, "BAM sort temporary directory.").unwrap();
        Sorter {
            reader,
            sorted_sink,
            thread_pool,
            mem_limit,
            out_compr_level, 
            buf_send,
            buf_recv,
            tmp_dir,
            tmp_file_counter: 0,
        }

    }

    fn read_split_sort_dump_chunks(&mut self) {
        let mut recs_buf = Some(RecordsBuffer::new(self.mem_limit / 2));
        // Preload replacement. While one buffer is sorted this one will be loaded with data.
        self.buf_send.send(RecordsBuffer::new(self.mem_limit / 2));

        while !self.reader.empty() {
            recs_buf.unwrap().clear();
            recs_buf.unwrap().fill(&mut self.reader);
            
            self.thread_pool.spawn(|| sort_chunk(recs_buf.take().unwrap(), buf_sender));
            recs_buf = Some(self.buf_recv.recv().unwrap());

            self.dump(recs_buf.as_deref().unwrap());
        }

        let last_buf = self.buf_recv.recv().unwrap();
        self.dump(last_buf);


    }

    fn dump(&mut self, buf: &RecordsBuffer) {
        let mut tmp_file = self.make_tmp_file(self.tmp_file_counter.to_string());
        self.tmp_file_counter += 1;

    }

    fn sort_chunk(buf: RecordsBuffer, buf_send: Sender<RecordsBuffer>) {

    }    

    fn make_tmp_file(&self, file_name: &str) -> std::io::Result<std::fs::File> {
        let file_path = self.tmp_dir.path().join(file_name);
        File::create(file_path)
    }
}