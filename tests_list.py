import test_suites

# Map containing test routines and files needed for them
# Reference hashes generated with https://github.com/NickRoz1/BAM-file-hash-generator
tests = {
    test_suites.test_match_mate_sort: [
        (
            "match_mates.bam",
            "https://github.com/biod/sambamba/raw/master/test/match_mates.bam",
            "dfc84d497d016ee477771ba35f56e1b6",
        ),
        (
            "test_parallel_bam_reader.bam",
            "http://hgdownload.cse.ucsc.edu/goldenPath/hg19/encodeDCC/wgEncodeUwRepliSeq/wgEncodeUwRepliSeqK562G1AlnRep1.bam",
            "f314004468f708c3cf989f5ccb55e827",
        ),
    ],
    test_suites.test_name_sort: [
        (
            "match_mates.bam",
            "https://github.com/biod/sambamba/raw/master/test/match_mates.bam",
            "2a770947df4addd3be9281b0e922cf88",
        ),
    ],
    test_suites.test_coord_sort: [
        (
            "match_mates.bam",
            "https://github.com/biod/sambamba/raw/master/test/match_mates.bam",
            "76bfa67e2092b500e122876bf7346e8c",
        ),
        (
            "test_parallel_bam_reader.bam",
            "http://hgdownload.cse.ucsc.edu/goldenPath/hg19/encodeDCC/wgEncodeUwRepliSeq/wgEncodeUwRepliSeqK562G1AlnRep1.bam",
            "563f30a5ecb85f5a3f77983072ec83bf",
        ),
    ],
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
