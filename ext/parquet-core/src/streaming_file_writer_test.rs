use super::{row_group_ordinal, MAX_ROW_GROUPS_PER_FILE};

#[test]
fn row_group_limit_is_checked_before_opening_another_segment() {
    assert_eq!(
        i16::MAX,
        row_group_ordinal(MAX_ROW_GROUPS_PER_FILE - 1).unwrap()
    );
    assert_eq!(
        format!(
            "Invalid argument: Parquet does not support more than \
             {MAX_ROW_GROUPS_PER_FILE} row groups per file"
        ),
        row_group_ordinal(MAX_ROW_GROUPS_PER_FILE)
            .unwrap_err()
            .to_string()
    );
}
