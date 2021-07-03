# Parallel BAM reader

This is a modificated, parallel version of [noodles BAM reader](https://github.com/zaeleus/noodles) written in **Rust**. The library also includes Rust rewrite of [sambamba parallel sort](https://github.com/biod/sambamba/blob/master/sambamba/sort.d). It currently supports 3 modes: sort by name, sort by name and match mates, sort by coordinate and strand. 

**Currently** sort just dumps sorted stream records bytes into file. One can direct sorted stream into any other BAM writer and obtain sorted BAM file.

## Installation

Add this to `Cargo.toml`:
```toml
[dependencies]
bam_parallel = { git = "https://github.com/NickRoz1/bam_parallel" }
```

## Testing

bam_tools crate utilizes python script to do testing. To test bam_tools:
```bash
cargo build --release
python3 test.py
```

## Usage

To read all records in BAM file and print them into `stdout`:

```rust
use bam_tools::Reader;

let mut bgzf_reader = Reader::new(reader, std::cmp::min(num_cpus::get(), 20));

bgzf_reader.read_header().unwrap();
bgzf_reader.consume_reference_sequences().unwrap();

let mut records = bgzf_reader.records();
while let Some(Ok(rec)) = records.next_rec() {
    println!("{:?}", rec);
}
```

To sort:
```bash
cargo build --release

# sort by name
./target/release/bam_binary -i -n test_files/input.bam -o test_files/out.bam 

# sort by name and match mates
./target/release/bam_binary -i -n -M test_files/input.bam -o test_files/out.bam 

# sort by coordinate and strand
./target/release/bam_binary -i test_files/input.bam -o test_files/out.bam 
```