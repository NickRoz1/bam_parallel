import subprocess
import os
import pathlib
import tempfile
from urllib.request import urlretrieve


# Generates md5 has sum via calling md5sum CLI
# Test hash is generated using md5sum (GNU coreutils) 8.26
def get_file_md5_sum(file_path):
    res = subprocess.run(["md5sum", file_path], capture_output=True)
    md5sum = res.split()[0]
    return md5sum


class test_tuple:
    def __init__(self, source_url, reference_hash, meta) -> None:
        self.source_url = source_url
        self.ref_hash = reference_hash
        self.meta = meta


def run_bam_parallel_and_check(path, command):
    res = subprocess.run([binary_path, "-q"] + command, capture_output=True)
    # Empty res means it has been run successfully since it's in quite mode
    print(res)
    assert res.returncode == 0


# MD5 test hash is generated for reference file generated via calling
# ../bin/sambamba-0.8.0-linux-amd64-static sort -t2 -n -M -m 300K match_mates.bam -o match_mates_nameSorted.bam
def test_match_mate_sort(binary_path, test_files):
    reference_hash = "a92acaf18901832885ae8c3d2fe696f0"
    temp_file = tempfile.NamedTemporaryFile()
    run_bam_parallel_and_check(
        binary_path,
        [
            "-n",
            "-M",
            "-i{}".format(test_files[0].name),
            "-o{}".format(temp_file.name),
        ],
    )
    return (
        get_file_md5_sum(temp_file.name) == "a92acaf18901832885ae8c3d2fe696f0"
    )


# Map containing test routines and files needed for them
tests = {
    test_match_mate_sort: [
        (
            "match_mates.bam",
            "https://github.com/biod/sambamba/raw/master/test/match_mates.bam",
        )
    ]
}


def fetch_if_does_not_exist(file_path):
    if not os.path.exists(file_path):
        print(
            "Downloading test file <{}> for <{}> test suite.".format(
                file_name, test.__name__
            )
        )
        urlretrieve(file_url, file_path)
        print("Download completed.")


if __name__ == "__main__":
    cur_dir = pathlib.Path().resolve()
    test_dir = os.path.join(cur_dir, "test_files")
    if not os.path.exists(test_dir):
        os.mkdir(test_dir)
    binary_path = "target/release/bam_binary"
    for (test, required_files) in tests.items():
        test_files_handles = []
        for (file_name, file_url) in required_files:
            file_path = os.path.join(test_dir, file_name)
            print(file_path)
            fetch_if_does_not_exist(file_path)
            test_files_handles.append(open(file_path))
        if not test(binary_path, test_files_handles):
            print("Test {} failed.".format(test.__name__))
        else:
            print("Test {} OK.".format(test.__name__))
    print("Tests completed.")
