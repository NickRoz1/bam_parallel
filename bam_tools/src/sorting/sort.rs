use crate::record::bamrawrecord::BAMRawRecord;
use crate::{Reader, MEGA_BYTE_SIZE};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crossbeam_channel::bounded;
use lz4_flex;
use rayon::prelude::ParallelSliceMut;
use rayon::{self, ThreadPoolBuilder};
use std::borrow::{Borrow, Cow};
use std::cmp::Reverse;
use std::slice::from_raw_parts;
use std::thread;

use super::comparators::{
    compare_coordinates_and_strand, compare_read_names, compare_read_names_and_mates, extract_key,
    KeyTuple,
};

use std::cmp::{max, min, Ordering};
use std::collections::BinaryHeap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tempdir::TempDir;

static mut io_wait: Duration = Duration::from_secs(0);

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

    pub fn fill(&mut self, reader: &mut Reader) -> std::io::Result<usize> {
        let now = Instant::now();
        self.clear();
        let mut last_byte_offset: usize = 0;
        loop {
            let rec_size = reader.append_record(&mut self.records_bytes)?;
            if rec_size == 0 {
                break;
            }
            // Push the range of bytes which this record occupies
            self.records
                .push(last_byte_offset..last_byte_offset + rec_size);
            last_byte_offset += rec_size;
            // println!("Buf size: {}", last_byte_offset);

            if self.records_bytes.len() > self.mem_limit {
                // Buffer has been filled.
                break;
            }
        }
        unsafe {
            io_wait += now.elapsed();
        }
        Ok(last_byte_offset)
    }
}

/// Which comparator to choose for sorting
#[derive(Clone, Copy, Debug)]
pub enum SortBy {
    Name,
    NameAndMatchMates,
    CoordinatesAndStrand,
}

pub enum TempFilesMode {
    RegularFiles,
    LZ4CompressedFiles,
    InMemoryBlocks,
    InMemoryBlocksLZ4,
}

/// Memory limit won't be strictly obeyed, but it probably won't be overflowed significantly.
pub fn sort_bam<R: Read + Send + 'static, W: Write>(
    mem_limit: usize,
    reader: R,
    sorted_sink: &mut W,
    tmp_dir_path: PathBuf,
    out_compr_level: usize,
    sort_thread_num: usize,
    reader_thread_num: usize,
    temp_files_mode: TempFilesMode,
    sort_by: SortBy,
) -> std::io::Result<()> {
    let decompress_thread_num = max(min(num_cpus::get(), sort_thread_num), 1);
    let reader_thread_num = max(min(num_cpus::get(), reader_thread_num), 1);

    let mut parallel_reader = Reader::new(reader, reader_thread_num);
    parallel_reader.read_header().unwrap();

    let tmp_dir = TempDir::new_in(tmp_dir_path, "BAM sort temporary directory.").unwrap();

    let now = Instant::now();
    let tmp_medium = read_split_sort_dump_chunks(
        &mut parallel_reader,
        mem_limit,
        sort_thread_num,
        &tmp_dir,
        &temp_files_mode,
        sort_by,
    );

    merge_sorted_chunks_and_write(
        mem_limit,
        tmp_medium,
        sort_by,
        sorted_sink,
        &temp_files_mode,
    )?;
    unsafe {
        println!(
            "Elapsed time without IO in Rust sort: {:?}",
            now.elapsed() - io_wait
        );
    }
    Ok(())
}

fn read_split_sort_dump_chunks(
    reader: &mut Reader,
    mem_limit: usize,
    sort_thread_num: usize,
    tmp_dir: &TempDir,
    temp_files_mode: &TempFilesMode,
    sort_by: SortBy,
) -> Vec<Box<dyn Read>> {
    let (work_send, work_receive) = bounded(1);
    let (result_send, result_receive) = bounded(1);
    let mut recs_buf = Some(RecordsBuffer::new(mem_limit / 2));

    let sort_thread_handle = thread::spawn(move || {
        for buf in work_receive {
            result_send.send(sort_chunk(buf, sort_by)).unwrap();
        }
    });

    let mut temp_medium = Vec::<Box<dyn Read>>::new();
    let mut temp_files_counter = 0;

    // Load first chunk to start the cycle.
    if let Ok(0) = recs_buf.as_mut().unwrap().fill(reader) {
        // Empty file
        return Vec::new();
    }

    let taken_buf = recs_buf.take().unwrap();
    work_send.send(taken_buf).unwrap();

    // While one buffer is being sorted this one will be loaded with data.
    recs_buf = Some(RecordsBuffer::new(mem_limit / 2));

    while let Ok(bytes_read) = recs_buf.as_mut().unwrap().fill(reader) {
        if bytes_read != 0 {
            let taken_buf = recs_buf.take().unwrap();
            work_send.send(taken_buf).unwrap();
        }

        recs_buf = Some(result_receive.recv().unwrap());

        if !recs_buf.as_ref().unwrap().records.is_empty() {
            match *temp_files_mode {
                TempFilesMode::RegularFiles | TempFilesMode::LZ4CompressedFiles => {
                    let file_name = temp_files_counter.to_string();
                    let mut temp_file = make_tmp_file(&file_name, &tmp_dir).unwrap();
                    dump(recs_buf.as_ref().unwrap(), &mut temp_file, temp_files_mode).unwrap();
                    temp_file.sync_all().unwrap();
                    temp_file.seek(SeekFrom::Start(0)).unwrap();
                    temp_medium.push(Box::new(temp_file));
                    temp_files_counter += 1;
                }
                TempFilesMode::InMemoryBlocks | TempFilesMode::InMemoryBlocksLZ4 => {
                    let mut vec = Vec::new();
                    // 1GB of BAM data occupies approximately 520-600 MB if compressed with LZ4.
                    vec.reserve(MEGA_BYTE_SIZE * 640);
                    let mut cursor = Cursor::new(vec);
                    dump(recs_buf.as_ref().unwrap(), &mut cursor, temp_files_mode).unwrap();
                    cursor.set_position(0);
                    temp_medium.push(Box::new(cursor));
                }
            }
        }

        if bytes_read == 0 {
            break;
        }
    }

    drop(work_send);
    sort_thread_handle.join().unwrap();

    temp_medium
}

fn dump<W: Write>(
    buf: &RecordsBuffer,
    sink: &mut W,
    temp_files_mode: &TempFilesMode,
) -> std::io::Result<()> {
    let now = Instant::now();
    match temp_files_mode {
        TempFilesMode::RegularFiles => {
            write(buf, &mut BufWriter::new(sink))?;
        }
        TempFilesMode::LZ4CompressedFiles => {
            let mut wrt = lz4_flex::frame::FrameEncoder::new(BufWriter::new(sink));
            write(buf, &mut wrt)?;
            wrt.finish().unwrap();
        }
        TempFilesMode::InMemoryBlocks => {
            write(buf, sink).unwrap();
        }
        TempFilesMode::InMemoryBlocksLZ4 => {
            let mut wrt = lz4_flex::frame::FrameEncoder::new(BufWriter::new(sink));
            write(buf, &mut wrt)?;
            wrt.finish().unwrap();
        }
    }
    // let mut buf_writer = BufWriter::new(file);
    // if compress_temp_files {
    //     let mut wrt = lz4_flex::frame::FrameEncoder::new(buf_writer);
    //     write(buf, &mut wrt)?;
    //     wrt.finish().unwrap();
    // } else {
    //     write(buf, &mut buf_writer)?;
    // }
    unsafe {
        io_wait += now.elapsed();
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
// the ranges to write byte ranges into file in sorted order.
fn sort_chunk(mut buf: RecordsBuffer, sort_by: SortBy) -> RecordsBuffer {
    let mut sorting_keys: Vec<(KeyTuple, Range<usize>)> = Vec::new();
    for range in buf.records {
        let rec_wrapper = BAMRawRecord(std::borrow::Cow::Borrowed(
            &buf.records_bytes[range.start..range.end],
        ));
        sorting_keys.push((
            extract_key(
                &rec_wrapper,
                &buf.records_bytes[range.start..range.end],
                &sort_by,
            ),
            range,
        ));
    }
    sorting_keys[..].par_sort_by(get_tuple_comparator(sort_by));
    // The BAMRawRecords and their corresponding ranges are now in
    // order. Replace original ranges with the sorted ones.
    buf.records = sorting_keys.into_iter().map(|rec| rec.1).collect();
    buf
}

fn get_tuple_comparator(
    sort_by: SortBy,
) -> impl Fn(&(KeyTuple, Range<usize>), &(KeyTuple, Range<usize>)) -> Ordering {
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

    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(file_path)
}

// Struct which manages reading chunks from files
struct ChunkReader {
    inner: Box<dyn Read>,
}

enum ChunkReaderStatus {
    ReachedEOF,
    LoadedRecord,
}
//
impl ChunkReader {
    // Creates new ChunkReader for temp file.
    pub fn new(reader: Box<dyn Read>) -> Self {
        Self { inner: reader }
    }
    // Reads bytes from inner reader into buffer.
    pub fn load_rec(&mut self, rec_buf: &mut Vec<u8>) -> std::io::Result<ChunkReaderStatus> {
        // Needed to check whether EOF is reached.
        let mut len_buf: [u8; 8] = [0; 8];
        match self.inner.read_exact(&mut len_buf[..]) {
            // EOF reached.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(ChunkReaderStatus::ReachedEOF)
            }
            Ok(()) => (),
            Err(e) => return Err(e),
        }
        let data_len = (&len_buf[..]).read_u64::<LittleEndian>()?;
        rec_buf.resize(data_len as usize, 0);
        self.inner.read_exact(&mut rec_buf[..])?;
        Ok(ChunkReaderStatus::LoadedRecord)
    }
}

type Comparator = fn(&KeyTuple, &KeyTuple) -> Ordering;

// Comparator field is used to order records in BinaryHeap. One can create
// wrapper structs and define Ord and PartialOrd traits for them, and then just
// pass generic parameters to BinaryHeap instead of this. But this solution is
// simpler.
struct MergeCandidate<'a> {
    key: KeyTuple<'a>,
    buf: Vec<u8>,
    provider_idx: usize,
    comparator: &'a Comparator,
}

impl<'a> MergeCandidate<'a> {
    pub fn new(
        buf: Vec<u8>,
        provider_idx: usize,
        comparator: &'a Comparator,
        sort_by: &SortBy,
    ) -> Self {
        let ptr = buf.as_ptr();
        let rec_bytes = unsafe { from_raw_parts(ptr, buf.len()) };
        let rec = BAMRawRecord(Cow::Borrowed(&rec_bytes[..]));
        let key = extract_key(&rec, &rec_bytes[..], sort_by);

        Self {
            key,
            buf,
            provider_idx,
            comparator,
        }
    }

    pub fn get_key(&self) -> &KeyTuple {
        &self.key
    }

    pub fn get_data(self) -> Vec<u8> {
        self.buf
    }
}

impl<'a> PartialEq for MergeCandidate<'a> {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self.comparator)(&self.get_key(), &other.get_key()),
            Ordering::Equal
        )
    }
}

impl<'a> Eq for MergeCandidate<'a> {}

// WARNING: the assumption is made that A < B and B < C means A < C
impl<'a> PartialOrd for MergeCandidate<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some((self.comparator)(&self.get_key(), &other.get_key()))
    }
}
impl<'a> Ord for MergeCandidate<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.comparator)(&self.get_key(), &other.get_key())
    }
}

// This struct handles merging of N sorted streams.
struct NWayMerger<'a> {
    providers: Vec<ChunkReader>,
    // The ChunkReaders cache records and have peek() in their API, so it's
    // possible to choose what record should go first (they contain record
    // inside themselves).
    min_heap: BinaryHeap<Reverse<MergeCandidate<'a>>>,
    sort_by: SortBy,
}

impl<'a> NWayMerger<'a> {
    pub fn new(
        chunks_readers: Vec<ChunkReader>,
        comparator: &'a Comparator,
        sort_by: SortBy,
    ) -> Self {
        let mut min_heap = BinaryHeap::new();
        let mut providers = Vec::<ChunkReader>::new();

        let mut provider_idx = 0;
        for mut chunk_reader in chunks_readers.into_iter() {
            let mut buf = Vec::<u8>::new();

            match chunk_reader.load_rec(&mut buf).unwrap() {
                ChunkReaderStatus::ReachedEOF => panic!("Temporary file exists but it's empty."),
                ChunkReaderStatus::LoadedRecord => {
                    min_heap.push(Reverse(MergeCandidate::new(
                        buf,
                        provider_idx,
                        comparator,
                        &sort_by,
                    )));
                    providers.push(chunk_reader);
                    provider_idx += 1;
                }
            }
        }

        Self {
            providers,
            min_heap,
            sort_by,
        }
    }

    /// Returns next record in order to merge. None if no more records left.
    pub fn get_next_rec(&mut self, mut used_buffer: Vec<u8>) -> Option<BAMRawRecord> {
        if self.min_heap.is_empty() {
            return None;
        }

        let cur_rec = self.min_heap.pop().unwrap().0;
        let rec_provider_idx = cur_rec.provider_idx;
        let rec_comparator = cur_rec.comparator;

        let now = Instant::now();
        // If ChunkReader reached EOF, don't put anything into min_heap so empty
        // ChunkReader won't be touched anymore.
        if let ChunkReaderStatus::LoadedRecord = self.providers[rec_provider_idx]
            .load_rec(&mut used_buffer)
            .unwrap()
        {
            unsafe {
                io_wait += now.elapsed();
            }
            self.min_heap.push(Reverse(MergeCandidate::new(
                used_buffer,
                rec_provider_idx,
                rec_comparator,
                &self.sort_by,
            )));
        }
        Some(BAMRawRecord(Cow::Owned(cur_rec.get_data())))
    }
}

/// This function might allocate more than allowed, because there are inner
/// buffers (not the reader buffer) which temporarily holds record.
fn merge_sorted_chunks_and_write<W: Write>(
    mem_limit: usize,
    tmp_medium: Vec<Box<dyn Read>>,
    sort_by: SortBy,
    writer: &mut W,
    temp_files_are_compressed: &TempFilesMode,
) -> std::io::Result<()> {
    let num_chunks = tmp_medium.len();
    let input_buf_mem_limit = min(16 * MEGA_BYTE_SIZE, mem_limit / 4 / num_chunks);

    let now = Instant::now();

    let mut chunks_readers = Vec::new();
    for tmp in tmp_medium {
        match temp_files_are_compressed {
            TempFilesMode::RegularFiles => {
                chunks_readers.push(ChunkReader::new(Box::new(BufReader::with_capacity(
                    input_buf_mem_limit,
                    tmp,
                ))));
            }
            TempFilesMode::LZ4CompressedFiles => {
                chunks_readers.push(ChunkReader::new(Box::new(
                    lz4_flex::frame::FrameDecoder::new(BufReader::with_capacity(
                        input_buf_mem_limit,
                        tmp,
                    )),
                )));
            }
            TempFilesMode::InMemoryBlocks => {
                chunks_readers.push(ChunkReader::new(tmp));
            }
            TempFilesMode::InMemoryBlocksLZ4 => {
                chunks_readers.push(ChunkReader::new(Box::new(
                    lz4_flex::frame::FrameDecoder::new(tmp),
                )));
            }
        }
    }

    let comparator = get_comparator(sort_by);
    let mut merger = NWayMerger::new(chunks_readers, &comparator, sort_by);

    let mut temp_buf = Vec::<u8>::new();

    unsafe {
        io_wait += now.elapsed();
    }
    let mut prev;

    while let Some(rec) = merger.get_next_rec(temp_buf) {
        prev = now.elapsed();
        writer.write_all(&rec.0[..])?;
        unsafe {
            io_wait += now.elapsed() - prev;
        }
        // Buffer rotation.
        temp_buf = rec.0.into_owned();
    }

    writer.flush()?;

    Ok(())
}
