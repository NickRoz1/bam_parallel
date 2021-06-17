use crate::{ParallelReader, MEGA_BYTE_SIZE};
// use crate::{GIGA_BYTE_SIZE, MEGA_BYTE_SIZE};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
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
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::mem;
use std::ops::Range;
use std::path::PathBuf;
use tempdir::TempDir;

/// This struct manages buffer for unsorted reads
// #[derive(Send)]
struct RecordsBuffer {
    mem_limit: usize,
    records_bytes: Vec<u8>,
    // Could be done this way with some unsafe code - because self referential
    // struct:
    // ---
    // This will hold "pointers" to actual data in buffers (to avoid
    // allocations). Since BAMRawRecord is a wrapper struct, this will help
    // access records data when sorting. If owned BAMRawRecord would be used (so
    // there is no need in records_bytes field), in case when there is record
    // bigger than previous the allocation will happen, in borrowed case just
    // more space will be occupied in the buffer and less records will fit in.
    // records: Vec<BAMRawRecord>,
    // ---
    // This will hold ranges corresponding to byte spans for each record in
    // records_bytes. When we'll need to operate on record fields, record wrappers will
    // be constructed on the fly.
    records: Vec<Range<usize>>,
}

impl RecordsBuffer {
    pub fn new(mem_limit: usize) -> Self {
        RecordsBuffer {
            mem_limit,
            records_bytes: vec![0; mem_limit],
            records: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.records.clear();
        self.records_bytes.clear();
    }

    pub fn fill<R>(&mut self, reader: &mut ParallelReader<R>) {
        self.clear();
        let mut last_byte_offset: usize = 0;
        let mut buf = &mut self.records_bytes[..];
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
            // Push the range of bytes which this record occupies
            self.records
                .push(last_byte_offset..last_byte_offset + block_size);
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

/// Memory limit won't be strictly obeyed, but it probably won't be overflowed significantly.
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

    let tmp_file_names = read_split_sort_dump_chunks(
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
) -> Vec<File> {
    let (buf_send, buf_recv) = flume::unbounded();
    let mut recs_buf = Some(RecordsBuffer::new(mem_limit / 2));

    buf_send.send(RecordsBuffer::new(mem_limit / 2)).unwrap();
    let tmp_dir = TempDir::new_in(tmp_dir_path, "BAM sort temporary directory.").unwrap();

    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(decompress_thread_num)
        .build()
        .unwrap();

    let mut temp_files = Vec::<File>::new();
    let mut temp_files_counter = 0;

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
        return Vec::new();
    }
    while !reader.empty() {
        recs_buf.as_mut().unwrap().clear();
        recs_buf.as_mut().unwrap().fill(reader);
        let taken_buf = recs_buf.take().unwrap();
        let send_channel = buf_send.clone();
        thread_pool.spawn(move || sort_chunk(taken_buf, send_channel, sort_by));
        recs_buf = Some(buf_recv.recv().unwrap());

        let file_name = temp_files_counter.to_string();
        let mut temp_file = make_tmp_file(&file_name, &tmp_dir).unwrap();
        dump(
            recs_buf.as_ref().unwrap(),
            &mut temp_file,
            compress_temp_files,
        )
        .unwrap();
        temp_files.push(temp_file);
        temp_files_counter += 1;
    }

    let last_buf = buf_recv.recv().unwrap();
    // One can optimize this so when there is no temp files written (there is
    // only one chunk in file) it's written straight to the resulting file, but
    // it's probably not worth it.
    let file_name = temp_files_counter.to_string();
    let mut temp_file = make_tmp_file(&temp_files_counter.to_string(), &tmp_dir).unwrap();
    dump(&last_buf, &mut temp_file, compress_temp_files).unwrap();
    temp_files.push(temp_file);

    // Ensure all buffered data is written
    temp_files
        .iter_mut()
        .for_each(|file| file.sync_all().unwrap());

    return temp_files;
}

fn dump(buf: &RecordsBuffer, file: &mut File, compress_temp_files: bool) -> std::io::Result<()> {
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
        let rec_size = (rec.end - rec.start) as u64;
        writer.write_u64::<LittleEndian>(rec_size)?;
        writer.write_all(&buf.records_bytes[rec.start..rec.end])?;
    }
    Ok(())
}

// For each byte range construct record wrapper, sort records and then reorder
// the ranges to then write byte ranges into file in sorted order.
fn sort_chunk(mut buf: RecordsBuffer, buf_return: Sender<RecordsBuffer>, sort_by: SortBy) {
    let mut record_wrappers: Vec<(BAMRawRecord, Range<usize>)> = buf
        .records
        .iter()
        .map(|range| {
            (
                BAMRawRecord(std::borrow::Cow::Borrowed(
                    &buf.records_bytes[range.start..range.end],
                )),
                range.clone(),
            )
        })
        .collect();
    &record_wrappers[..].par_sort_by(get_tuple_comparator(sort_by));
    // The BAMRawRecords and their corresponding ranges are now in
    // order. Replace original ranges with the sorted ones.
    buf.records = record_wrappers.into_iter().map(|rec| rec.1).collect();
    buf_return.send(buf).unwrap();
}

fn get_tuple_comparator(
    sort_by: SortBy,
) -> impl Fn(&(BAMRawRecord, Range<usize>), &(BAMRawRecord, Range<usize>)) -> Ordering {
    let cmp = get_comparator(sort_by);
    move |a, b| cmp(&a.0, &b.0)
}

fn get_comparator(sort_by: SortBy) -> Comparator {
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

type Comparator = fn(&BAMRawRecord, &BAMRawRecord) -> Ordering;
// Struct which manages reading chunks from files
struct ChunkReader {
    inner: BufReader<File>,
    buf: Option<Vec<u8>>,
    // There would be some problems with lifetimes if doing it this way. Before
    // putting ChunkReaders into min heap next_rec has to be called, and it
    // borrows the self while the ChunkReader is moved into min_heap.
    // rec: Option<BAMRawRecord<'a>>

    // This field is used to order peeked records in
    // BinaryHeap. One can create wrapper structs and define Ord and PartialOrd
    // traits for them, and then just pass generic parameters to BinaryHeap
    // instead of this. But this solution is simpler.
    comparator: Comparator,
}

impl ChunkReader {
    // Creates new ChunkReader for temp file.
    pub fn new(tmp_file: File, mem_limit: usize, comparator: Comparator) -> Self {
        let mut chunk_reader = Self {
            inner: BufReader::with_capacity(mem_limit, tmp_file),
            buf: Some(Vec::new()),
            comparator,
        };
        // Preload first record.
        chunk_reader
            .next_rec()
            .expect("Temp file exists but it's empty.");
        chunk_reader
    }
    // Reads bytes from inner reader into inner buffer, discarding previous record.
    pub fn next_rec<'a>(&'a mut self) -> std::io::Result<Option<BAMRawRecord<'a>>> {
        // Needed to check whether EOF is reached.
        let mut len_buf: [u8; 8] = [0; 8];
        if self.inner.read(&mut len_buf[..])? == 0 {
            // EOF reached.
            self.buf = None;
            return Ok(None);
        }
        let data_len = (&len_buf[..]).read_u64::<LittleEndian>()?;
        let rec_buf = self.buf.as_mut().unwrap();
        rec_buf.resize(data_len as usize, 0);
        self.inner.read_exact(&mut rec_buf[..])?;
        Ok(Some(BAMRawRecord(std::borrow::Cow::Borrowed(&rec_buf[..]))))
    }

    // Return reference to last loaded BAMRawRecord or None if the reader reached EOF.
    pub fn peek<'a>(&'a self) -> Option<BAMRawRecord<'a>> {
        self.buf
            .as_ref()
            .and_then(|buf| Some(BAMRawRecord(std::borrow::Cow::Borrowed(&buf[..]))))
    }
}

impl PartialEq for ChunkReader {
    fn eq(&self, other: &Self) -> bool {
        match (self.comparator)(&self.peek().unwrap(), &other.peek().unwrap()) {
            Ordering::Equal => true,
            _ => false,
        }
    }
}

impl Eq for ChunkReader {}

// WARNING: the assumption is made that A < B and B < C means A < C
impl PartialOrd for ChunkReader {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some((self.comparator)(
            &self.peek().unwrap(),
            &other.peek().unwrap(),
        ))
    }
}
impl Ord for ChunkReader {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.comparator)(&self.peek().unwrap(), &other.peek().unwrap())
    }
}

// This struct handles merging of N sorted streams.
struct NWayMerger {
    // The ChunkReaders cache records and have peek() in their API, so it's
    // possible to choose what record should go first (they contain record
    // inside themselves).
    min_heap: BinaryHeap<ChunkReader>,
}

impl NWayMerger {
    pub fn new(chunks_readers: Vec<ChunkReader>) -> Self {
        let mut min_heap = BinaryHeap::new();
        chunks_readers
            .into_iter()
            .for_each(|chunk_reader| min_heap.push(chunk_reader));
        Self { min_heap }
    }
    /// Returns next record in order to merge. None if no more records left.
    pub fn get_next_rec(&mut self) -> Option<BAMRawRecord> {
        if self.min_heap.is_empty() {
            return None;
        }
        let cur_rec = self.min_heap.peek().unwrap().peek();
        self.min_heap.peek().unwrap().next_rec();
    }
}

fn merge_sorted_chunks_and_write<W: Write>(
    mem_limit: usize,
    tmp_files: Vec<File>,
    sort_by: SortBy,
    writer: &mut W,
) -> std::io::Result<()> {
    let num_chunks = tmp_files.len();
    let input_buf_mem_limit = min(16 * MEGA_BYTE_SIZE, mem_limit / 4 / num_chunks);

    // ChunkReader might allocate more than allowed, because there is an inner
    // buffer (not the reader buffer) which temporarily holds read record.
    let chunks_readers: Vec<ChunkReader> = tmp_files
        .into_iter()
        .map(|tmp_file| ChunkReader::new(tmp_file, input_buf_mem_limit, get_comparator(sort_by)))
        .collect();

    Ok(())
}
