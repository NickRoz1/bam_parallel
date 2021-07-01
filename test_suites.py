import tempfile


# MD5 test hash is generated for reference file generated via calling
# ../bin/sambamba-0.8.0-linux-amd64-static sort -t2 -n -M -m 300K match_mates.bam -o match_mates_nameSorted.bam
def test_match_mate_sort(binary_wrapper, test_files):
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


def test_parallel_bam_reader(binary_wrapper, test_files):
    for test_file in test_files:
        hash = binary_wrapper.exec(["-h", "-i", test_file.get_path()], False)
        if not test_file.check_hash(hash):
            return False
    return True
