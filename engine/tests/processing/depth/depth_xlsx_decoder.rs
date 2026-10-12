#![cfg(feature = "process")]

use std::io::{Cursor, Write};

use zip::{ZipWriter, write::SimpleFileOptions};

use marketforge_engine::decode::XlsxDepthDecoder;

fn make_xlsx(worksheet: &str) -> Cursor<Vec<u8>> {
    let buffer = Cursor::new(Vec::new());
    let mut writer = ZipWriter::new(buffer);

    writer
        .start_file("xl/worksheets/sheet1.xml", SimpleFileOptions::default())
        .unwrap();

    writer.write_all(worksheet.as_bytes()).unwrap();

    writer.finish().unwrap()
}

fn worksheet(rows: &str) -> String {
    format!(
        r#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
            <sheetData>
                <row r="1">
                    <c r="A1" t="inlineStr"><is><t>timestamp</t></is></c>
                    <c r="B1" t="inlineStr"><is><t>asks</t></is></c>
                    <c r="C1" t="inlineStr"><is><t>bids</t></is></c>
                </row>
                {rows}
            </sheetData>
        </worksheet>"#
    )
}

#[test]
fn decodes_and_sorts_xlsx_snapshots() {
    let worksheet = r#"
        <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
          <sheetData>
            <row r="1">
              <c r="A1" t="inlineStr"><is><t>timestamp</t></is></c>
              <c r="B1" t="inlineStr"><is><t>asks</t></is></c>
              <c r="C1" t="inlineStr"><is><t>bids</t></is></c>
            </row>
            <row r="2">
              <c r="A2"><v>200</v></c>
              <c r="B2" t="inlineStr"><is><t>[[101.1,2.5]]</t></is></c>
              <c r="C2" t="inlineStr"><is><t>[[100.1,3.5]]</t></is></c>
            </row>
            <row r="3">
              <c r="A3"><v>100</v></c>
              <c r="B3" t="inlineStr"><is><t>[[102.1,4.5]]</t></is></c>
              <c r="C3" t="inlineStr"><is><t>[[99.1,5.5]]</t></is></c>
            </row>
          </sheetData>
        </worksheet>
    "#;

    let buffer = Cursor::new(Vec::new());

    let mut writer = ZipWriter::new(buffer);

    writer
        .start_file("xl/worksheets/sheet1.xml", SimpleFileOptions::default())
        .unwrap();

    writer.write_all(worksheet.as_bytes()).unwrap();

    let buffer = writer.finish().unwrap();

    let records = XlsxDepthDecoder::decode(buffer).unwrap();

    assert_eq!(records.len(), 2);

    assert_eq!(records[0].timestamp, 100);
    assert_eq!(records[1].timestamp, 200);

    assert_eq!(records[0].source_ordinal, 2);
    assert_eq!(records[1].source_ordinal, 1);

    assert_eq!(records[0].record["asks"][0][0], "102.1");

    assert_eq!(records[1].record["bids"][0][1], "3.5");
}

#[test]
fn decodes_reordered_cells() {
    let xml = worksheet(
        r#"
        <row r="2">
            <c r="C2" t="inlineStr">
                <is><t>[[100.1,3.5]]</t></is>
            </c>
            <c r="A2"><v>100</v></c>
            <c r="B2" t="inlineStr">
                <is><t>[[101.1,2.5]]</t></is>
            </c>
        </row>
        "#,
    );

    let records = XlsxDepthDecoder::decode(make_xlsx(&xml)).unwrap();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].timestamp, 100);

    assert_eq!(records[0].record["asks"][0][0], "101.1");
    assert_eq!(records[0].record["bids"][0][0], "100.1");
}

#[test]
fn rejects_missing_columns() {
    let xml = worksheet(
        r#"
        <row r="2">
            <c r="A2"><v>100</v></c>
            <c r="B2" t="inlineStr">
                <is><t>[[101.1,2.5]]</t></is>
            </c>
        </row>
        "#,
    );

    assert!(XlsxDepthDecoder::decode(make_xlsx(&xml)).is_err());
}

#[test]
fn rejects_duplicate_cells() {
    let xml = worksheet(
        r#"
        <row r="2">
            <c r="A2"><v>100</v></c>
            <c r="A2"><v>200</v></c>
            <c r="B2" t="inlineStr">
                <is><t>[[101.1,2.5]]</t></is>
            </c>
            <c r="C2" t="inlineStr">
                <is><t>[[100.1,3.5]]</t></is>
            </c>
        </row>
        "#,
    );

    assert!(XlsxDepthDecoder::decode(make_xlsx(&xml)).is_err());
}

#[test]
fn preserves_source_order_for_duplicate_timestamps() {
    let xml = worksheet(
        r#"
        <row r="2">
            <c r="A2"><v>100</v></c>
            <c r="B2" t="inlineStr"><is><t>[[101,1]]</t></is></c>
            <c r="C2" t="inlineStr"><is><t>[[99,1]]</t></is></c>
        </row>

        <row r="3">
            <c r="A3"><v>100</v></c>
            <c r="B3" t="inlineStr"><is><t>[[102,2]]</t></is></c>
            <c r="C3" t="inlineStr"><is><t>[[98,2]]</t></is></c>
        </row>
        "#,
    );

    let records = XlsxDepthDecoder::decode(make_xlsx(&xml)).unwrap();

    assert_eq!(records.len(), 2);

    assert_eq!(records[0].source_ordinal, 1);
    assert_eq!(records[1].source_ordinal, 2);

    assert_eq!(records[0].timestamp, 100);
    assert_eq!(records[1].timestamp, 100);
}

#[test]
fn preserves_decimal_precision() {
    let xml = worksheet(
        r#"
        <row r="2">
            <c r="A2"><v>100</v></c>

            <c r="B2" t="inlineStr">
                <is><t>[[101.123456789012345678,0.000000123456789012345678]]</t></is>
            </c>

            <c r="C2" t="inlineStr">
                <is><t>[[100.123456789012345678,1.123456789012345678]]</t></is>
            </c>
        </row>
        "#,
    );

    let records = XlsxDepthDecoder::decode(make_xlsx(&xml)).unwrap();

    assert_eq!(records[0].record["asks"][0][0], "101.123456789012345678");

    assert_eq!(
        records[0].record["asks"][0][1],
        "0.000000123456789012345678"
    );

    assert_eq!(records[0].record["bids"][0][1], "1.123456789012345678");
}
