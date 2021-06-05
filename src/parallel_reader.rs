use super::{fetch_block, inflate_data, Block};
use flume;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::{self, Read};

#[derive(Default)]
struct WorkUnit {
    // Number of this WorkUnit in parsed blocks "queue". Less the number - sooner
    // the contents loaded into the buffer block.
    number: usize,
    // TODO: preallocate blocks
    cdata: Vec<u8>,
    block: Block,
    block_size: usize,
}

impl Ord for WorkUnit {
    fn cmp(&self, other: &Self) -> Ordering {
        // Smallest go first.
        other.number.cmp(&self.number)
    }
}

impl PartialOrd for WorkUnit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Smallest go first.
        Some(self.cmp(other))
    }
}

impl Eq for WorkUnit {}

impl PartialEq for WorkUnit {
    fn eq(&self, other: &Self) -> bool {
        // There shouldn't be two WorkUnits with the same number in the Heap
        assert_ne!(self.number, other.number);
        false
    }
}

#[derive(Debug)]
enum Suspend {
    EOF,
    // The reader's cursor will be moved.
    Seek,
}
enum MessageToController {
    Read(WorkUnit),
    Suspend(Suspend),
}

enum ReaderStatus<T> {
    Continue(T),
    Terminate,
}

impl Into<Option<WorkUnit>> for MessageToController {
    fn into(self) -> Option<WorkUnit> {
        match self {
            MessageToController::Read(w) => Some(w),
            _ => None,
        }
    }
}

/// Acts as a threadpool to decompress BGZF blocks.
pub struct ParallelReader<T> {
    worker_handlers: Vec<Option<std::thread::JoinHandle<()>>>,
    controller_handle: Option<std::thread::JoinHandle<()>>,
    // Manages blocks buffering (correct ordering).
    block_sorter_thread: Option<std::thread::JoinHandle<()>>,
    // Used to send reader to controller thread.
    controller_t: flume::Sender<ReaderStatus<T>>,
    // Used to send reader to main thread.
    reader_r: flume::Receiver<T>,
    // Used to get unpacked blocks.
    consumer_r: flume::Receiver<Option<WorkUnit>>,
    // Used to return unpacked blocks.WorkUnit
    work_unit_processor_t: flume::Sender<MessageToController>,
    block_buffer: Block,
    position: u64,
}

impl<T: Read + Send + 'static> ParallelReader<T> {
    /// New
    pub fn new(inner: T, mut thread_num: usize) -> Self {
        thread_num = std::cmp::min(num_cpus::get(), thread_num);

        let mut worker_handlers = Vec::<Option<std::thread::JoinHandle<()>>>::new();

        let (controller_t, controller_r): (
            flume::Sender<ReaderStatus<T>>,
            flume::Receiver<ReaderStatus<T>>,
        ) = flume::unbounded();
        let (worker_t, worker_r): (flume::Sender<WorkUnit>, flume::Receiver<WorkUnit>) =
            flume::unbounded();
        let (block_sorter_t, block_sorter_r): (flume::Sender<WorkUnit>, flume::Receiver<WorkUnit>) =
            flume::unbounded();
        let (work_unit_processor_t, work_unit_processor_r): (
            flume::Sender<MessageToController>,
            flume::Receiver<MessageToController>,
        ) = flume::unbounded();
        let (reader_t, reader_r) = flume::unbounded();
        let (consumer_t, consumer_r): (
            flume::Sender<Option<WorkUnit>>,
            flume::Receiver<Option<WorkUnit>>,
        ) = flume::unbounded();

        // Initialize decompressing threads.
        for _ in 0..thread_num {
            let worker_r_clone = worker_r.clone();
            let sorter_sink = block_sorter_t.clone();
            worker_handlers.push(Some(std::thread::spawn(move || {
                while let Ok(mut work_unit) = worker_r_clone.recv() {
                    let udata = work_unit.block.data_mut();
                    let udata_buf = udata.get_mut();
                    work_unit.block_size = inflate_data(&work_unit.cdata[..], udata_buf)
                        .expect("Failed to inflate data.");
                    udata.set_position(0);
                    sorter_sink.send(work_unit).unwrap();
                }
            })));
        }

        let sink_t = consumer_t.clone();
        // Manages block buffers.
        let block_sorter_thread = std::thread::spawn(move || {
            // The heap is needed for cases when the blocks are not inflated in
            // proper order (as coming from input stream).
            let mut block_heap = BinaryHeap::<WorkUnit>::new();
            // Number of current block (ordered as read from input stream).
            let mut cur_block_num = 0;
            while let Ok(work_unit) = block_sorter_r.recv() {
                block_heap.push(work_unit);
                // Fill queue with parsed blocks.
                while !block_heap.is_empty() && (block_heap.peek().unwrap().number == cur_block_num)
                {
                    sink_t.send(Some(block_heap.pop().unwrap())).unwrap();
                    // The block is extracted. Wait for next one.
                    cur_block_num += 1;
                }
            }
        });

        // Used to sink unused work_units when reader is suspended.
        let consumer_r_sink = consumer_r.clone();
        controller_t.send(ReaderStatus::Continue(inner)).unwrap();
        let processor_sink = work_unit_processor_t.clone();

        // Reader thread. Loads tasks with data from input stream and dispatches them.
        let controller_handle = std::thread::spawn(move || {
            // Number of current block (to order work_units as they were read from input stream).
            let mut cur_block_num = 0;
            // Stores workers when reader is paused on EOF to avoid reallocations.
            let mut worker_storage: Vec<WorkUnit> =
                (0..thread_num).map(|_| WorkUnit::default()).collect();
            // Waits for available reader.
            while let Ok(ReaderStatus::Continue(mut reader)) = controller_r.recv() {
                assert_eq!(
                    worker_storage.len(),
                    thread_num,
                    "{} work units have been lost.",
                    thread_num - worker_storage.len()
                );
                // Drain work_units saved on suspend into processing queue (to
                // be filled and dispatched to decompressing threads).
                worker_storage.drain(..).for_each(|e| {
                    processor_sink
                        .send(MessageToController::Read(e))
                        .expect("Failed to push into queue.")
                });

                loop {
                    // Waits for signal.
                    match work_unit_processor_r.recv() {
                        Ok(MessageToController::Read(mut work_unit)) => {
                            match fetch_block(
                                &mut reader,
                                &mut work_unit.cdata,
                                &mut work_unit.block,
                            ) {
                                // Reached EOF.
                                Ok(0) => {
                                    // Message the work_unit_processor to
                                    // suspend activity and return reader to
                                    // main thread for further manipulations.
                                    processor_sink
                                        .send(MessageToController::Suspend(Suspend::EOF))
                                        .expect("FLUME channel interaction failed.");
                                    processor_sink
                                        .send(MessageToController::Read(work_unit))
                                        .expect("FLUME channel interaction failed.");
                                }
                                Ok(_) => {
                                    // This is to track the order of parsed
                                    // work_units. Even if block read second was
                                    // processed faster than first one, it will
                                    // be returned to consumer second.
                                    work_unit.number = cur_block_num;
                                    cur_block_num += 1;
                                    worker_t
                                        .send(work_unit)
                                        .expect("FLUME channel interaction failed.");
                                }
                                Err(e) => panic!("Failed to fetch block, error: {}", e),
                            }
                        }
                        Ok(MessageToController::Suspend(suspend_type)) => {
                            // Save all work_units to reuse after reader is
                            // resumed. There is one work_unit per thread.
                            while worker_storage.len() != thread_num {
                                let work_unit: Option<WorkUnit> = match suspend_type {
                                    // If EOF is reached, the uncompressed
                                    // work_units are still needed since they
                                    // may be consumed. We wait till
                                    // work_unit comes through full cycle (and can
                                    // be consumed).
                                    Suspend::EOF => work_unit_processor_r
                                        .recv()
                                        .expect("FLUME channel interaction failed.")
                                        .into(),
                                    // If inner reader is requested to seek, the
                                    // work_units may be disregarded since the
                                    // consumer no longer needs work units
                                    // coming from that input stream area. We
                                    // intercept work_units straight from
                                    // consumer queue.
                                    Suspend::Seek => consumer_r_sink
                                        .recv()
                                        .expect("FLUME channel interaction failed."),
                                };
                                if let Some(unit) = work_unit {
                                    worker_storage.push(unit);
                                }
                            }
                            if let Suspend::EOF = suspend_type {
                                // Signal consumer that stream has been exhausted.
                                consumer_t
                                    .send(None)
                                    .expect("FLUME channel interaction failed.");
                            }
                            // Return reader to the main thread (to seek).
                            reader_t
                                .send(reader)
                                .expect("FLUME channel interaction failed.");
                            break;
                        }
                        Err(_) => {
                            eprintln!("ERROR");
                            break;
                        }
                    }
                }
            }
        });

        Self {
            worker_handlers: worker_handlers,
            controller_handle: Some(controller_handle),
            block_sorter_thread: Some(block_sorter_thread),
            controller_t: controller_t,
            reader_r: reader_r,
            consumer_r: consumer_r,
            work_unit_processor_t: work_unit_processor_t,
            // TODO: Buffer_BLOCK NOT BLOCK BUFFER
            block_buffer: Default::default(),
            position: 0,
        }
    }
}

impl<T> Read for ParallelReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Attempt to fill buf from current block
        match self.block_buffer.data_mut().read(buf) {
            // Block exhausted, get new.
            Ok(0) => {
                match self.consumer_r.recv().unwrap() {
                    // EOF
                    None => Ok(0),
                    // New block has been read. Continue reading.
                    Some(mut work_unit) => {
                        let block_size = work_unit.block_size;
                        // Get new block.
                        std::mem::swap(&mut self.block_buffer, &mut work_unit.block);
                        // Send thread buffer back.
                        self.work_unit_processor_t
                            .send(MessageToController::Read(work_unit))
                            .unwrap();

                        self.block_buffer.set_position(self.position);
                        // TODO: this value should be altered on seek reader requests.
                        self.position += block_size as u64;
                        // https://rust-lang.github.io/rfcs/0980-read-exact.html#about-errorkindinterrupted
                        Err(io::Error::from(io::ErrorKind::Interrupted))
                    }
                }
            }
            Ok(n) => Ok(n),
            Err(e) => Err(e),
        }
    }
}

impl<T> Drop for ParallelReader<T> {
    fn drop(&mut self) {
        self.work_unit_processor_t
            .send(MessageToController::Suspend(Suspend::Seek))
            .unwrap();
        self.controller_t.send(ReaderStatus::Terminate).unwrap();

        self.controller_handle.take().unwrap().join().unwrap();
        self.block_sorter_thread.take().unwrap().join().unwrap();
        for handle in &mut self.worker_handlers {
            handle.take().unwrap().join().unwrap();
        }
    }
}
