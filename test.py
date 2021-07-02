import os
import pathlib
import test_suites
import subprocess
from urllib.request import urlretrieve


class bam_parallel_binary:
    def __init__(self, binary_path):
        self.binary_path = binary_path

    def exec(self, command, quiet=True):
        if quiet:
            command += ["-q"]
        res = subprocess.run([self.binary_path] + command, capture_output=True)
        assert res.returncode == 0, res
        return res.stdout.strip().decode("utf-8")


# Generates md5 has sum via calling md5sum CLI
# Test hash is generated using md5sum (GNU coreutils) 8.26
def get_file_md5_sum(file_path):
    res = subprocess.run(["md5sum", file_path], capture_output=True)
    md5sum = res.split()[0]
    return md5sum


def fetch_if_does_not_exist(file_name, file_path, file_url):
    if not os.path.exists(file_path):
        print(
            "Downloading test file <{}> for <{}> test suite.".format(
                file_name, test.__name__
            )
        )
        urlretrieve(file_url, file_path)
        print("Download completed.")


class test_tuple:
    def __init__(self, test_dir, file_name, source_url, reference_hash) -> None:
        self.file_name = file_name
        self.source_url = source_url
        self.ref_hash = reference_hash
        self.path = os.path.join(test_dir, file_name)

    def get_handle(self):
        fetch_if_does_not_exist(self.file_name, self.path, self.source_url)
        return open(self.path)

    def get_path(self):
        fetch_if_does_not_exist(self.file_name, self.path, self.source_url)
        return self.path

    def check_hash(self, hash):
        return self.ref_hash == hash


# Map containing test routines and files needed for them
# Reference hashes generated with https://github.com/NickRoz1/BAM-file-hash-generator
tests = {
    # test_suites.test_match_mate_sort: [
    #     (
    #         "match_mates.bam",
    #         "https://github.com/biod/sambamba/raw/master/test/match_mates.bam",
    #         "dfc84d497d016ee477771ba35f56e1b6",
    #     )
    # ],
    test_suites.test_parallel_bam_reader: [
        (
            "test_parallel_bam_reader.bam",
            "http://hgdownload.cse.ucsc.edu/goldenPath/hg19/encodeDCC/wgEncodeUwRepliSeq/wgEncodeUwRepliSeqK562G1AlnRep1.bam",
            "17c8bc5770e48dde225fd804c4bad013",
        ),
        (
            "match_mates.bam",
            "https://github.com/biod/sambamba/raw/master/test/match_mates.bam",
            "390cdd7a23488217351161daa9f59736",
        ),
    ],
}

if __name__ == "__main__":
    cur_dir = pathlib.Path().resolve()
    test_dir = os.path.join(cur_dir, "test_files")
    if not os.path.exists(test_dir):
        os.mkdir(test_dir)
    binary_path = "target/release/bam_binary"
    binary_wrapper = bam_parallel_binary(binary_path)
    for (test, required_files) in tests.items():
        test_files = []
        for reqs in required_files:
            test_files.append(test_tuple(test_dir, *reqs))
        if not test(binary_wrapper, test_files):
            print("Test {} failed.".format(test.__name__))
        else:
            print("Test {} OK.".format(test.__name__))
    print("Tests completed.")
