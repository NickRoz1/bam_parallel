import os
import pathlib
import tests_list
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


if __name__ == "__main__":
    cur_dir = pathlib.Path().resolve()
    test_dir = os.path.join(cur_dir, "test_files")
    if not os.path.exists(test_dir):
        os.mkdir(test_dir)
    binary_path = "target/release/bam_binary"
    binary_wrapper = bam_parallel_binary(binary_path)
    for (test, required_files) in tests_list.tests.items():
        test_files = []
        for reqs in required_files:
            test_files.append(test_tuple(test_dir, *reqs))
        if not test(binary_wrapper, test_files):
            print("Test {} failed.".format(test.__name__))
        else:
            print("Test {} OK.".format(test.__name__))
    print("Tests completed.")
