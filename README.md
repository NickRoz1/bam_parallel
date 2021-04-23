# Parallel BAM reader

This is a modificated, parallel version of [noodles BAM reader](https://github.com/zaeleus/noodles) written in **Rust**. It currently doesn't support any advanced operations on data and should be used just as fast BGZF unpacker.

## Installation

Add this to `Cargo.toml`:
```toml
[dependencies]
bam_parallel = { git = "https://github.com/NickRoz1/bam_parallel" }
```

## Usage

To read all records in BAM file and print them into `stdout`:

```rust
use bam_parallel::ParallelReader;
use std::fs::File;
use byteorder::{LittleEndian, ReadBytesExt};

let fin = File::open("path/to/file.bam").expect("failed");
let buf_reader = std::io::BufReader::new(fin);
let mut reader = ParallelReader::new(buf_reader, 10);

/// This can be just a regular Vec::<u8>
let mut buf = RawRecord::from(Vec::<u8>::new());

let mut magic = [0; 4];
reader.read_exact(&mut magic).expect("Failed to read.");

if &magic[..] != b"BAM\x01" {
    panic!("invalid BAM header");
}

let l_text = reader.read_u32::<LittleEndian>().expect("Failed to read.");
let mut c_text = vec![0; l_text as usize];
reader.read_exact(&mut c_text).expect("Failed to read.");

let n_ref = reader.read_u32::<LittleEndian>().expect("Failed to read.");

for _ in 0..n_ref {
    let l_name = reader.read_u32::<LittleEndian>().expect("Failed to read.");
    let mut c_name = vec![0; l_name as usize];
    reader.read_exact(&mut c_name).expect("Failed to read.");
    reader.read_u32::<LittleEndian>().expect("Failed to read.");
}

loop {
    let block_size = match reader.read_u32::<LittleEndian>() {
        Ok(bs) => bs as usize,
        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => 0,
        Err(e) => panic!(e),
    };
    if block_size == 0 {
        writer.finish();
        return ();
    }

    buf.resize(block_size);
    reader.read_exact(&mut buf).expect("FAILED TO READ.");

    println!("{:?}", buf);
}
```