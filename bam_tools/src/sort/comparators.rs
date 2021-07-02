use super::flags;
use byteorder::{LittleEndian, ReadBytesExt};
use gbam_tools::BAMRawRecord;
use gbam_tools::Fields;
use std::cmp::Ordering;
use std::str::from_utf8;

pub fn compare_read_names(lhs: &BAMRawRecord, rhs: &BAMRawRecord) -> Ordering {
    let name_lhs = from_utf8(lhs.get_bytes(&Fields::ReadName)).unwrap();
    let name_rhs = from_utf8(rhs.get_bytes(&Fields::ReadName)).unwrap();
    return name_lhs.cmp(name_rhs);
}

/// Comparison function for 'queryname' sorting order setting mates
/// of the same alignments adjacent with the first mate coming before
/// the second mate
pub fn compare_read_names_and_mates(lhs: &BAMRawRecord, rhs: &BAMRawRecord) -> Ordering {
    let ordering = compare_read_names(lhs, rhs);
    if ordering == Ordering::Equal {
        let tag_lhs = get_tag_val(lhs);
        let tag_rhs = get_tag_val(rhs);

        if tag_lhs == tag_rhs {
            // Source https://github.com/biod/sambamba/blob/c795656721b3608ffe7765b6ab98502426d14131/BioD/bio/std/hts/bam/read.d#L1603
            let flag_lhs = get_flag_val(lhs);
            let flag_rhs = get_flag_val(rhs);
            return flag_lhs.cmp(&flag_rhs);
        }
        return tag_lhs.unwrap().cmp(&tag_rhs.unwrap());
    }
    return ordering;
}

fn get_flag_val(rec: &BAMRawRecord) -> u16 {
    rec.get_bytes(&Fields::Flags)
        .read_u16::<LittleEndian>()
        .unwrap()
}

fn get_tag_val(rec: &BAMRawRecord) -> Option<i32> {
    rec.get_hit_count()
}

pub fn compare_coordinates_and_strand(lhs: &BAMRawRecord, rhs: &BAMRawRecord) -> Ordering {
    let refid_lhs = get_ref_or_pos_id(lhs, &Fields::RefID);
    let refid_rhs = get_ref_or_pos_id(rhs, &Fields::RefID);
    if refid_lhs == -1 {
        return Ordering::Greater;
    }
    if refid_rhs == -1 {
        return Ordering::Less;
    }
    if refid_lhs != refid_lhs {
        return refid_lhs.cmp(&refid_rhs);
    }
    let pos_lhs = get_ref_or_pos_id(lhs, &Fields::Pos);
    let pos_rhs = get_ref_or_pos_id(rhs, &Fields::Pos);
    if pos_lhs != pos_rhs {
        return pos_lhs.cmp(&pos_rhs);
    }
    let is_reverse_strand_lhs = is_reverse_strand(lhs);
    let is_reverse_strand_rhs = is_reverse_strand(rhs);
    if !is_reverse_strand_lhs && is_reverse_strand_rhs {
        return Ordering::Less;
    } else {
        return Ordering::Greater;
    }
}

fn is_reverse_strand(rec: &BAMRawRecord) -> bool {
    let flags = rec
        .get_bytes(&Fields::Flags)
        .read_u16::<LittleEndian>()
        .unwrap();
    let bit_field = flags::Flags::from_bits(flags).unwrap();
    bit_field.is_reverse_complemented()
}

fn get_ref_or_pos_id(rec: &BAMRawRecord, field: &Fields) -> i32 {
    rec.get_bytes(field).read_i32::<LittleEndian>().unwrap()
}
