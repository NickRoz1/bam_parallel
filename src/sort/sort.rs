use crate::ParallelReader;
// use crate::{GIGA_BYTE_SIZE, MEGA_BYTE_SIZE};
use flume::Sender;
use gbam_tools::BAMRawRecord;
use lz4_flex;
use rayon::prelude::ParallelSliceMut;
use rayon::{self, ThreadPoolBuilder};

use super::comparators::{
    compare_coordinates_and_strand, compare_read_names, compare_read_names_and_mates,
};
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::cmp::{max, min, Ordering};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;
use tempdir::TempDir;

/// This struct manages buffer for unsorted reads
// #[derive(Send)]
struct RecordsBuffer<'a> {
    mem_limit: usize,
    records_bytes: RefCell<Vec<u8>>,
    // This will hold "pointers" to actual data in buffers (to avoid
    // allocations). Since BAMRawRecord is a wrapper struct, this will help
    // access records data when sorting. If owned BAMRawRecord would be used (so
    // there is no need in records_bytes field), in case when there is record
    // bigger than previous the allocation will happen, in borrowed case just
    // more space will be occupied in the buffer and less records will fit in.
    records: Vec<BAMRawRecord<'a>>,
}

impl<'a> RecordsBuffer<'a> {
    pub fn new(mem_limit: usize) -> Self {
        RecordsBuffer {
            mem_limit,
            records_bytes: RefCell::new(vec![0; mem_limit]),
            records: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.records.clear();
        self.records_bytes.borrow_mut().clear();
    }

    pub fn fill<R>(&'a mut self, reader: &mut ParallelReader<R>) {
        self.clear();
        let mut last_byte_offset: usize = 0;
        let mut buf = &mut self.records_bytes.borrow_mut()[..];
        loop {
            let block_size = reader.read_block_size().unwrap();
            // We have to read at least something in the buffer, hence '> 0' check.
            if last_byte_offset > 0 && last_byte_offset + block_size > self.mem_limit {
                // Buffer has been filled.
                break;
            }
            reader
                .read_exact(&mut buf[last_byte_offset..last_byte_offset + block_size])
                .unwrap();
            // This is done to convince borrow checker that we do not borrow
            // same part of slice twice.
            let (loaded_data, rest_of_the_buffer) = buf.split_at_mut(last_byte_offset + block_size);
            // We don't need first part of the buffer anymore, since it's already filled.
            buf = rest_of_the_buffer;
            self.records
                .push(BAMRawRecord(std::borrow::Cow::Borrowed(loaded_data)));
            last_byte_offset += block_size;
        }
    }
}

/// Which comparator to choose for sorting
#[derive(Clone, Copy)]
pub enum SortBy {
    Name,
    NameAndMatchMates,
    CoordinatesAndStrand,
}

pub fn sort_bam<R: Read + Send + 'static, W>(
    mem_limit: usize,
    reader: R,
    sorted_sink: W,
    tmp_dir_path: PathBuf,
    out_compr_level: usize,
    mut decompress_thread_num: usize,
    mut reader_thread_num: usize,
    compress_temp_files: bool,
    sort_by: SortBy,
) {
    let decompress_thread_num = max(min(num_cpus::get(), decompress_thread_num), 1);
    let reader_thread_num = max(min(num_cpus::get(), reader_thread_num), 1);

    let mut parallel_reader = ParallelReader::new(reader, reader_thread_num);

    let tmp_file_num = read_split_sort_dump_chunks(
        &mut parallel_reader,
        mem_limit,
        decompress_thread_num,
        tmp_dir_path,
        compress_temp_files,
        sort_by,
    );
}

fn read_split_sort_dump_chunks<R: Read + Send + 'static>(
    reader: &mut ParallelReader<R>,
    mem_limit: usize,
    decompress_thread_num: usize,
    tmp_dir_path: PathBuf,
    compress_temp_files: bool,
    sort_by: SortBy,
) -> usize {
    let (buf_send, buf_recv) = flume::unbounded();
    let mut recs_buf = Some(RecordsBuffer::<'static>::new(mem_limit / 2));

    buf_send.send(RecordsBuffer::new(mem_limit / 2)).unwrap();
    let tmp_dir = TempDir::new_in(tmp_dir_path, "BAM sort temporary directory.").unwrap();

    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(decompress_thread_num)
        .build()
        .unwrap();

    let mut temp_files_counter: usize = 0;

    // Load first chunk to start the cycle.
    if !reader.empty() {
        recs_buf.as_mut().unwrap().fill(reader);
        let taken_buf = recs_buf.take().unwrap();
        let send_channel = buf_send.clone();
        thread_pool.spawn(move || sort_chunk(taken_buf, send_channel, sort_by));
        // While one buffer is being sorted this one will be loaded with data.
        recs_buf = Some(RecordsBuffer::new(mem_limit / 2));
    } else {
        // Empty file
        return 0;
    }
    while !reader.empty() {
        // recs_buf.as_mut().unwrap().clear();
        // recs_buf.as_mut().unwrap().fill(reader);
        let taken_buf = recs_buf.take().unwrap();
        let send_channel = buf_send.clone();
        thread_pool.spawn(move || sort_chunk(taken_buf, send_channel, sort_by));
        recs_buf = Some(buf_recv.recv().unwrap());

        let temp_file = make_tmp_file(&temp_files_counter.to_string(), &tmp_dir).unwrap();
        dump(recs_buf.as_ref().unwrap(), temp_file, compress_temp_files).unwrap();
        temp_files_counter += 1;
    }

    let last_buf = buf_recv.recv().unwrap();
    // One can optimize this so when there is no temp files written (there is
    // only one chunk in file) it's written straight to the resulting file, but
    // it's probably not worth it.
    let temp_file = make_tmp_file(&temp_files_counter.to_string(), &tmp_dir).unwrap();
    dump(&last_buf, temp_file, compress_temp_files).unwrap();
    temp_files_counter += 1;

    return temp_files_counter;
}

fn dump(buf: &RecordsBuffer, file: File, compress_temp_files: bool) -> std::io::Result<()> {
    let mut buf_writer = BufWriter::new(file);
    if compress_temp_files {
        let mut wrt = lz4_flex::frame::FrameEncoder::new(buf_writer);
        write(buf, &mut wrt)?;
        wrt.finish().unwrap();
    } else {
        write(buf, &mut buf_writer)?;
    }
    Ok(())
}

fn write<W: Write>(buf: &RecordsBuffer, writer: &mut W) -> std::io::Result<()> {
    for rec in &buf.records {
        writer.write_all(&rec.0)?;
    }
    Ok(())
}

fn sort_chunk<'a>(
    mut buf: RecordsBuffer<'a>,
    buf_return: Sender<RecordsBuffer<'a>>,
    sort_by: SortBy,
) {
    &buf.records[..].par_sort_by(get_comparator(sort_by));
    buf_return.send(buf).unwrap();
}

fn get_comparator(sort_by: SortBy) -> fn(&BAMRawRecord, &BAMRawRecord) -> Ordering {
    match sort_by {
        SortBy::Name => compare_read_names,
        SortBy::NameAndMatchMates => compare_read_names_and_mates,
        SortBy::CoordinatesAndStrand => compare_coordinates_and_strand,
    }
}

fn make_tmp_file(file_name: &str, tmp_dir: &TempDir) -> std::io::Result<std::fs::File> {
    let file_path = tmp_dir.path().join(file_name);
    File::create(file_path)
}
