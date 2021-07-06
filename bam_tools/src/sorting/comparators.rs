use super::flags;
use crate::record::bamrawrecord::BAMRawRecord;
use crate::record::fields::Fields;
use byteorder::{LittleEndian, ReadBytesExt};
use std::cmp::Ordering;
use std::str::from_utf8;

pub fn compare_read_names(lhs: &BAMRawRecord, rhs: &BAMRawRecord) -> Ordering {
    let name_lhs = from_utf8(lhs.get_bytes(&Fields::ReadName)).unwrap();
    let name_rhs = from_utf8(rhs.get_bytes(&Fields::ReadName)).unwrap();
    name_lhs.cmp(name_rhs)
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
    ordering
}

fn get_flag_val(rec: &BAMRawRecord) -> u16 {
    rec.get_bytes(&Fields::Flags)
        .read_u16::<LittleEndian>()
        .unwrap()
}

fn get_tag_val(rec: &BAMRawRecord) -> Option<i32> {
    rec.get_hit_count()
}

pub fn compare_coordinates_and_strand(left: &BAMRawRecord, right: &BAMRawRecord) -> Ordering {
    let refid_left = get_ref_or_pos_id(left, &Fields::RefID);
    let refid_right = get_ref_or_pos_id(right, &Fields::RefID);
    if refid_left == -1 {
        return Ordering::Greater;
    }
    if refid_right == -1 {
        return Ordering::Less;
    }
    if refid_left != refid_right {
        return refid_left.cmp(&refid_right);
    }
    let pos_left = get_ref_or_pos_id(left, &Fields::Pos);
    let pos_right = get_ref_or_pos_id(right, &Fields::Pos);
    if pos_left != pos_right {
        return pos_left.cmp(&pos_right);
    }
    let is_reverse_strand_left = is_reverse_strand(left);
    let is_reverse_strand_right = is_reverse_strand(right);
    if !is_reverse_strand_left && is_reverse_strand_right {
        Ordering::Less
    } else {
        Ordering::Greater
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
