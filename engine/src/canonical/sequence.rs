#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SequenceMetadata {
    pub sequence_first: Option<u64>,
    pub sequence_last: Option<u64>,
    pub sequence_previous: Option<u64>,
    pub cross_sequence: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_metadata_can_represent_partial_source_metadata() {
        let sequence = SequenceMetadata {
            sequence_first: Some(100),
            sequence_last: Some(105),
            sequence_previous: None,
            cross_sequence: None,
        };

        assert_eq!(sequence.sequence_first, Some(100));
        assert_eq!(sequence.sequence_last, Some(105));
    }
}
