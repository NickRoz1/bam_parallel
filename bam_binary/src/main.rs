use bam_tools::sorting::sort::sort_bam;
use bam_tools::sorting::sort::SortBy;
use bam_tools::Reader;
use bam_tools::MEGA_BYTE_SIZE;
use md5::{Digest, Md5};
use std::env;

use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "BAM parallel CLI")]
struct Opt {
    #[structopt(short = "q", long)]
    quite_mode: bool,

    /// Input file
    #[structopt(short = "i", parse(from_os_str))]
    input: PathBuf,

    /// Output file, stdout if not present
    #[structopt(short = "o", parse(from_os_str))]
    output: Option<PathBuf>,

    #[structopt(short = "n", long = "--sort-by-name")]
    sort_by_name: bool,

    #[structopt(short = "M", long = "--match-mates")]
    match_mates: bool,

    #[structopt(short = "h", long = "--calc-hash")]
    calc_hash: bool,
}

fn main() {
    let opt = Opt::from_args();

    let file = File::open(opt.input).unwrap();
    let reader = BufReader::new(file);

    if opt.calc_hash {
        println!("{}", generate_file_hash(reader));
        return;
    }

    let mut sort_by = SortBy::CoordinatesAndStrand;
    if opt.match_mates && !opt.sort_by_name {
        panic!("Cannot match mates when sorting by coordinates.");
    }
    if opt.sort_by_name {
        sort_by = match opt.match_mates {
            true => SortBy::NameAndMatchMates,
            false => SortBy::Name,
        }
    }

    let out_file = File::create(opt.output.unwrap()).unwrap();
    let mut writer = BufWriter::new(out_file);
    let tmp_dir_path = env::temp_dir();

    sort_bam(
        2000 * MEGA_BYTE_SIZE,
        reader,
        &mut writer,
        tmp_dir_path,
        0,
        5,
        5,
        false,
        sort_by,
    )
    .unwrap();

    std::process::exit(0);
}

fn generate_file_hash<R: Read + Send + 'static>(reader: R) -> String {
    let mut bgzf_reader = Reader::new(reader, std::cmp::min(num_cpus::get(), 20));

    let mut hasher = Md5::new();

    bgzf_reader.read_header().unwrap();
    bgzf_reader.consume_reference_sequences().unwrap();

    let mut records = bgzf_reader.records();
    while let Some(Ok(rec)) = records.next_rec() {
        hasher.update(&rec[..]);
    }

    let result = hasher.finalize();

    // https://stackoverflow.com/a/67070521
    format!("{:x}", result)
}
