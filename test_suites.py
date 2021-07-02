import tempfile


# MD5 test hash is generated for reference file generated via calling
# ../bin/sambamba-0.8.0-linux-amd64-static sort -t2 -n -M -m 300K match_mates.bam -o match_mates_nameSorted.bam
def test_match_mate_sort(binary_wrapper, test_files):
    for test_file in test_files:
        temp_file = tempfile.NamedTemporaryFile()
        binary_wrapper.exec(
            ["-n", "-M", "-i", test_file.get_path(), "-o", temp_file.name]
        )
        hash = binary_wrapper.exec(["-h", "-i", temp_file.name], False)
        if not test_file.check_hash(hash):
            return False
    return True


def test_parallel_bam_reader(binary_wrapper, test_files):
    for test_file in test_files:
        hash = binary_wrapper.exec(["-h", "-i", test_file.get_path()], False)
        if not test_file.check_hash(hash):
            return False
    return True
