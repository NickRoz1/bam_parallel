use crate::ParallelReader;
use crate::{GIGA_BYTE_SIZE, MEGA_BYTE_SIZE};
use flume::{Receiver, Sender};
use gbam_tools::BAMRawRecord;
use rayon::{self, ThreadBuilder, ThreadPoolBuilder};
use std::cmp::{max, min};
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::PathBuf;
use tempdir::TempDir;

/// This struct manages buffer for unsorted reads
struct RecordsBuffer {
    mem_limit: usize,
    records_bytes: Vec<u8>,
    records: // ? BamRecord should be able own and borrow underlying data to own as true wrapper struct
}

impl RecordsBuffer {
    pub fn new(mem_limit: usize) -> Self {
        RecordsBuffer {
            mem_limit,
            records_bytes: vec![0; mem_limit],
        }
    }

    pub fn clear(&mut self) {
        self.records_bytes.clear();
    }

    pub fn fill<R>(&mut self, reader: &mut ParallelReader<R>) {
        let mut last_byte_offset: usize = 0;
        loop {
            let block_size = reader.read_block_size().unwrap();
            if last_byte_offset + block_size > self.mem_limit {
                // Buffer has been filled.
                break;
            }
            let buf = &mut self.records_bytes[last_byte_offset..last_byte_offset + block_size];
            reader.read_exact(buf).unwrap();
        }
    }
}

struct Sorter<R, W> {
    reader: ParallelReader<R>,
    sorted_sink: W,
    thread_pool: rayon::ThreadPool,
    mem_limit: usize,
    out_compr_level: usize,
    buf_send: Sender<RecordsBuffer>,
    buf_recv: Receiver<RecordsBuffer>,
    tmp_dir: TempDir,
    tmp_file_counter: usize,
}

pub fn sort_bam<R: Read + Send + 'static, W>(
    mem_limit: usize,
    reader: R,
    sorted_sink: W,
    tmp_dir_path: PathBuf,
    out_compr_level: usize,
    mut decompress_thread_num: usize,
    mut reader_thread_num: usize,
) -> std::io::Result<()> {
    let decompress_thread_num = max(min(num_cpus::get(), decompress_thread_num), 1);
    let reader_thread_num = max(min(num_cpus::get(), reader_thread_num), 1);

    let parallel_reader = ParallelReader::new(reader, reader_thread_num);

    let tmp_file_num = read_split_sort_dump_chunks(
        &mut parallel_reader,
        mem_limit,
        decompress_thread_num,
        tmp_dir_path,
    );
}

fn read_split_sort_dump_chunks<R: Read + Send + 'static>(
    reader: &mut ParallelReader<R>,
    mem_limit: usize,
    decompress_thread_num: usize,
    tmp_dir_path: PathBuf,
) -> usize {
    let (buf_send, buf_recv) = flume::unbounded();
    let mut recs_buf = Some(RecordsBuffer::new(mem_limit / 2));

    buf_send.send(RecordsBuffer::new(mem_limit / 2));
    let tmp_dir = TempDir::new_in(tmp_dir_path, "BAM sort temporary directory.").unwrap();

    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(decompress_thread_num)
        .build()
        .unwrap();

    let temp_files_counter: usize = 0;

    // Load first chunk to start the cycle.
    if !reader.empty() {
        recs_buf.unwrap().fill(&mut reader);
        thread_pool.spawn(|| sort_chunk(recs_buf.take().unwrap(), buf_send.clone()));
        // While one buffer is being sorted this one will be loaded with data.
        recs_buf = Some(RecordsBuffer::new(mem_limit / 2));
    } else {
        // Empty file
        return 0;
    }
    while !reader.empty() {
        recs_buf.unwrap().clear();
        recs_buf.unwrap().fill(&mut reader);

        thread_pool.spawn(|| sort_chunk(recs_buf.take().unwrap(), buf_send.clone()));
        recs_buf = Some(buf_recv.recv().unwrap());

        let temp_file = make_tmp_file(&temp_files_counter.to_string(), &tmp_dir).unwrap();
        dump(recs_buf.as_ref().unwrap(), temp_file).unwrap();
    }

    let last_buf = buf_recv.recv().unwrap();
    let temp_file = make_tmp_file(&temp_files_counter.to_string(), &tmp_dir).unwrap();
    dump(&last_buf, temp_file).unwrap();
    temp_files_counter += 1;

    return temp_files_counter;
}

fn dump(buf: &RecordsBuffer, mut file: File) -> std::io::Result<()> {}

fn sort_chunk(buf: RecordsBuffer, buf_send: Sender<RecordsBuffer>) {}

fn make_tmp_file(file_name: &str, tmp_dir: &TempDir) -> std::io::Result<std::fs::File> {
    let file_path = tmp_dir.path().join(file_name);
    File::create(file_path)
}
