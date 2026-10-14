use arrow_schema::DataType;

use marketforge_engine::process::{
    convert::output::converted_combined_schema, merge::physical_combined_schema,
};

#[test]
fn combined_conversion_preserves_source_fields() {
    let source = physical_combined_schema();
    let converted = converted_combined_schema(&source);

    for (index, field) in source.fields().iter().enumerate() {
        assert_eq!(converted.field(index), field.as_ref(),);
    }

    assert_eq!(converted.fields().len(), source.fields().len() + 8,);
}

#[test]
fn combined_conversion_has_nullable_target_payloads() {
    let source = physical_combined_schema();
    let converted = converted_combined_schema(&source);

    let trade = converted.field_with_name("trade_target").unwrap();
    let depth = converted.field_with_name("depth_target").unwrap();

    assert!(trade.is_nullable());
    assert!(depth.is_nullable());

    assert!(matches!(trade.data_type(), DataType::Struct(_)));
    assert!(matches!(depth.data_type(), DataType::List(_)));
}

#[test]
fn combined_conversion_preserves_event_discriminator() {
    let source = physical_combined_schema();
    let converted = converted_combined_schema(&source);

    let event_kind = converted.field_with_name("event_kind").unwrap();

    assert_eq!(event_kind.data_type(), &DataType::UInt8);
    assert!(!event_kind.is_nullable());
}
