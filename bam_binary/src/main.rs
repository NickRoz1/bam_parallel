use bam_tools::sort::sort::sort_bam;
use bam_tools::sort::sort::SortBy;
use bam_tools::MEGA_BYTE_SIZE;
use std::env;

use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
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
    output: PathBuf,

    #[structopt(short = "n", long = "--sort-by-name")]
    sort_by_name: bool,

    #[structopt(short = "M", long = "--match-mates")]
    match_mates: bool,
}

fn main() {
    let opt = Opt::from_args();

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

    let file = File::open(opt.input).unwrap();
    let reader = BufReader::new(file);
    let out_file = File::create(opt.output).unwrap();
    let mut writer = BufWriter::new(out_file);
    let tmp_dir_path = env::temp_dir();

    sort_bam(
        MEGA_BYTE_SIZE,
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
