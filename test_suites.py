import tempfile
import subprocess

# Generates md5 has sum via calling md5sum CLI
# Test hash is generated using md5sum (GNU coreutils) 8.26
def get_file_md5_sum(file_path):
    res = subprocess.run(["md5sum", file_path], capture_output=True)
    assert res.returncode == 0, res
    md5sum = res.stdout.strip().split()[0].decode("utf-8")
    return md5sum


def test_match_mate_sort(binary_wrapper, test_files):
    return test_sort(binary_wrapper, test_files, ["-n", "-M"])


def test_name_sort(binary_wrapper, test_files):
    return test_sort(binary_wrapper, test_files, ["-n"])


def test_coord_sort(binary_wrapper, test_files):
    return test_sort(binary_wrapper, test_files, [])


def test_sort(binary_wrapper, test_files, options):
    for test_file in test_files:
        temp_file = tempfile.NamedTemporaryFile()
        binary_wrapper.exec(
            options + ["-i", test_file.get_path(), "-o", temp_file.name]
        )
        hash = get_file_md5_sum(temp_file.name)
        if not test_file.check_hash(hash):
            return False
    return True


def test_parallel_bam_reader(binary_wrapper, test_files):
    for test_file in test_files:
        hash = binary_wrapper.exec(["-h", "-i", test_file.get_path()], False)
        if not test_file.check_hash(hash):
            return False
    return True
