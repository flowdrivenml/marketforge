use std::io::{Cursor, Read, Seek};

use crate::shared::historical::limits;
use quick_xml::{Reader, XmlVersion, events::Event};
use serde_json::{Value, json};
use zip::ZipArchive;

use crate::{
    shared::error::{MarketForgeError, Result},
    shared::validation::IntegrityCategory,
};

#[derive(Debug)]
pub struct XlsxDepthRecord {
    pub source_ordinal: u64,
    pub timestamp: i64,
    pub record: Value,
}

pub struct XlsxDepthDecoder;

impl XlsxDepthDecoder {
    pub fn decode<R: Read + Seek>(reader: R) -> Result<Vec<XlsxDepthRecord>> {
        Self::decode_observed(reader, |_, _, _| Ok(()))
    }
    pub fn decode_observed<R: Read + Seek>(
        reader: R,
        mut observe: impl FnMut(u64, Option<i64>, Option<&MarketForgeError>) -> Result<()>,
    ) -> Result<Vec<XlsxDepthRecord>> {
        let mut workbook = ZipArchive::new(reader)
            .map_err(|error| invalid_error(format!("cannot open XLSX workbook: {error}")))?;

        let shared_strings = read_shared_strings(&mut workbook)?;

        let sheet_name = find_worksheet(&mut workbook)?;

        let mut sheet = workbook
            .by_name(&sheet_name)
            .map_err(|error| invalid_error(format!("cannot open worksheet: {error}")))?;

        let mut xml = Vec::new();

        read_xml(&mut sheet, &mut xml, "worksheet XML")?;

        let rows = parse_worksheet(&xml, &shared_strings)?;

        let mut records = Vec::new();

        for (ordinal, row) in rows.into_iter().enumerate() {
            if ordinal == 0 && row.first().map(String::as_str) == Some("timestamp") {
                continue;
            }

            let source_ordinal = u64::try_from(records.len())
                .map_err(|_| invalid_error("XLSX source ordinal overflow"))?
                + 1;

            let mut timestamp_ns = None;
            let result = (|| -> Result<XlsxDepthRecord> {
                if row.len() != 3 {
                    return invalid(format!(
                        "XLSX row {source_ordinal}: expected 3 columns, received {}",
                        row.len()
                    ));
                }

                let timestamp = row[0].parse::<i64>().map_err(|_| {
                    invalid_error(format!("XLSX row {source_ordinal}: invalid timestamp"))
                })?;

                timestamp_ns = timestamp.checked_mul(1_000_000_000);
                let asks = parse_levels(&row[1], source_ordinal, "asks")?;
                let bids = parse_levels(&row[2], source_ordinal, "bids")?;

                Ok(XlsxDepthRecord {
                    source_ordinal,
                    timestamp,
                    record: json!({
                        "timestamp": timestamp.to_string(),
                        "asks": asks,
                        "bids": bids,
                    }),
                })
            })();
            if matches!(result, Err(MarketForgeError::ResourceLimit(_))) {
                return result.map(|_| records);
            }
            observe(source_ordinal, timestamp_ns, result.as_ref().err())?;
            check_items(records.len(), "decoded workbook rows")?;
            records.push(result?);
        }

        records.sort_by_key(|record| (record.timestamp, record.source_ordinal));

        Ok(records)
    }
}

fn find_worksheet<R: Read + Seek>(workbook: &mut ZipArchive<R>) -> Result<String> {
    let mut sheet = None;
    let mut count = 0usize;
    for name in workbook.file_names() {
        let name = name.map_err(|e| invalid_error(format!("invalid XLSX member name: {e}")))?;
        if name.starts_with("xl/worksheets/sheet") && name.ends_with(".xml") {
            count = count
                .checked_add(1)
                .ok_or_else(|| invalid_error("worksheet count overflow"))?;
            if sheet.is_none() {
                sheet = Some(name.into_owned());
            }
        }
    }
    if count != 1 {
        return invalid(format!(
            "expected exactly one XLSX worksheet, found {count}"
        ));
    }
    Ok(sheet.expect("one worksheet"))
}

fn read_shared_strings<R: Read + Seek>(workbook: &mut ZipArchive<R>) -> Result<Vec<String>> {
    let Ok(mut file) = workbook.by_name("xl/sharedStrings.xml") else {
        return Ok(Vec::new());
    };

    let mut xml = Vec::new();

    read_xml(&mut file, &mut xml, "shared strings XML")?;

    let mut reader = Reader::from_reader(Cursor::new(xml));

    let mut buffer = Vec::new();
    let mut strings = Vec::new();

    let mut current = String::new();
    let mut inside_string = false;
    let mut inside_text = false;

    loop {
        match reader
            .read_event_into(&mut buffer)
            .map_err(|error| invalid_error(error.to_string()))?
        {
            Event::Start(event) if event.name().as_ref() == "si" => {
                current.clear();
                inside_string = true;
            }

            Event::Start(event) if event.name().as_ref() == "t" => {
                inside_text = true;
            }

            Event::Text(event) if inside_string && inside_text => {
                let text = event.xml_content(XmlVersion::Implicit1_0);

                let text = quick_xml::escape::unescape(&text)
                    .map_err(|error| invalid_error(error.to_string()))?;

                check_text(current.len(), text.len(), "shared string text")?;
                current.push_str(&text);
            }

            Event::End(event) if event.name().as_ref() == "t" => {
                inside_text = false;
            }

            Event::End(event) if event.name().as_ref() == "si" => {
                check_items(strings.len(), "shared strings")?;
                strings.push(std::mem::take(&mut current));
                inside_string = false;
            }

            Event::Eof => break,

            _ => {}
        }

        buffer.clear();
    }

    Ok(strings)
}

fn parse_worksheet(xml: &[u8], shared_strings: &[String]) -> Result<Vec<Vec<String>>> {
    use std::collections::BTreeMap;

    let mut reader = Reader::from_reader(Cursor::new(xml));
    let mut buffer = Vec::new();

    let mut rows = Vec::new();
    let mut current_row = BTreeMap::<usize, String>::new();
    let mut retained_text = 0u64;

    let mut current_value = String::new();
    let mut current_type = String::new();
    let mut current_column = None;
    let mut current_row_number: Option<u32> = None;

    let mut inside_value = false;
    let mut inside_cell = false;
    let mut inside_row = false;

    loop {
        match reader
            .read_event_into(&mut buffer)
            .map_err(|error| invalid_error(error.to_string()))?
        {
            Event::Start(event) if event.name().as_ref() == "row" => {
                if inside_row {
                    return invalid("nested XLSX worksheet rows");
                }

                current_row.clear();
                current_row_number = None;

                for attribute in event.attributes() {
                    let attribute = attribute.map_err(|error| invalid_error(error.to_string()))?;

                    if attribute.key.as_ref() == "r" {
                        let text = attribute.value.as_ref();

                        let number = text
                            .parse::<u32>()
                            .map_err(|_| invalid_error("invalid XLSX row number"))?;

                        if number == 0 {
                            return invalid("XLSX row number must be positive");
                        }

                        current_row_number = Some(number);
                    }
                }

                if current_row_number.is_none() {
                    return invalid("XLSX row missing row number");
                }

                inside_row = true;
            }

            Event::Start(event) if event.name().as_ref() == "c" => {
                if !inside_row || inside_cell {
                    return invalid("invalid XLSX cell structure");
                }

                current_value.clear();
                current_type.clear();
                current_column = None;

                for attribute in event.attributes() {
                    let attribute = attribute.map_err(|error| invalid_error(error.to_string()))?;

                    match attribute.key.as_ref() {
                        "r" => {
                            let reference = attribute.value.as_ref();
                            let (column, row_number) = parse_cell_reference(reference)?;

                            if Some(row_number) != current_row_number {
                                return invalid(format!(
                                    "XLSX cell {reference} does not belong to worksheet row {:?}",
                                    current_row_number
                                ));
                            }

                            current_column = Some(column);
                        }

                        "t" => {
                            current_type = attribute.value.as_ref().to_owned();
                        }

                        _ => {}
                    }
                }

                if current_column.is_none() {
                    return invalid("XLSX cell missing reference");
                }

                inside_cell = true;
            }

            Event::Start(event)
                if inside_cell
                    && (event.name().as_ref() == "v" || event.name().as_ref() == "t") =>
            {
                inside_value = true;
            }

            Event::Text(event) if inside_value => {
                let text = event.xml_content(XmlVersion::Implicit1_0);

                let text = quick_xml::escape::unescape(&text)
                    .map_err(|error| invalid_error(error.to_string()))?;

                check_text(current_value.len(), text.len(), "worksheet cell text")?;
                current_value.push_str(&text);
            }

            Event::End(event) if event.name().as_ref() == "v" || event.name().as_ref() == "t" => {
                inside_value = false;
            }

            Event::End(event) if event.name().as_ref() == "c" => {
                if !inside_cell {
                    return invalid("XLSX cell closed without opening");
                }

                let column = current_column
                    .take()
                    .ok_or_else(|| invalid_error("XLSX cell missing column"))?;

                let value = if current_type == "s" {
                    let index = current_value
                        .parse::<usize>()
                        .map_err(|_| invalid_error("invalid XLSX shared-string index"))?;

                    let text = shared_strings
                        .get(index)
                        .ok_or_else(|| invalid_error("XLSX shared-string index out of range"))?;
                    check_text(0, text.len(), "worksheet shared cell")?;
                    check_retained_text(retained_text, text.len())?;
                    text.clone()
                } else {
                    std::mem::take(&mut current_value)
                };

                check_items(current_row.len(), "worksheet cells")?;
                retained_text = check_retained_text(retained_text, value.len())?;
                if current_row.insert(column, value).is_some() {
                    return invalid(format!("duplicate XLSX cell in column {column}",));
                }

                inside_cell = false;
            }

            Event::End(event) if event.name().as_ref() == "row" => {
                if !inside_row || inside_cell {
                    return invalid("invalid XLSX row termination");
                }

                let expected_columns = [0, 1, 2];

                for column in expected_columns {
                    if !current_row.contains_key(&column) {
                        return invalid(format!("XLSX row missing column {}", column + 1,));
                    }
                }

                if current_row.len() != 3 {
                    return invalid(format!(
                        "XLSX row contains {} columns; expected 3",
                        current_row.len(),
                    ));
                }

                check_items(rows.len(), "worksheet rows")?;
                rows.push(vec![
                    current_row.remove(&0).unwrap(),
                    current_row.remove(&1).unwrap(),
                    current_row.remove(&2).unwrap(),
                ]);

                inside_row = false;
            }

            Event::Eof => {
                if inside_row || inside_cell || inside_value {
                    return invalid("unexpected EOF inside XLSX worksheet");
                }

                break;
            }

            _ => {}
        }

        buffer.clear();
    }

    Ok(rows)
}

fn parse_levels(text: &str, ordinal: u64, side: &str) -> Result<Value> {
    let levels: Value = serde_json::from_str(text).map_err(|error| {
        invalid_error(format!("XLSX row {ordinal}: invalid {side} JSON: {error}"))
    })?;

    let array = levels
        .as_array()
        .ok_or_else(|| invalid_error(format!("XLSX row {ordinal}: {side} must be an array")))?;

    limits::depth(array.len(), "workbook level normalization")?;
    let mut normalized = Vec::with_capacity(array.len());

    for level in array {
        let fields = level
            .as_array()
            .ok_or_else(|| invalid_error("XLSX depth level must be an array"))?;

        if fields.len() != 2 {
            return invalid("XLSX depth level must contain price and quantity");
        }

        normalized.push(json!([
            numeric_text(&fields[0])?,
            numeric_text(&fields[1])?,
        ]));
    }

    Ok(Value::Array(normalized))
}

fn numeric_text(value: &Value) -> Result<String> {
    match value {
        Value::String(text) => Ok(text.clone()),

        Value::Number(number) => Ok(number.to_string()),

        _ => invalid("XLSX depth value must be numeric"),
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::RecordIntegrity {
        category: IntegrityCategory::InvalidRecord,
        message: message.into(),
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}

fn parse_cell_reference(reference: &str) -> Result<(usize, u32)> {
    let split = reference
        .find(|character: char| !character.is_ascii_alphabetic())
        .ok_or_else(|| invalid_error("XLSX cell reference missing row"))?;

    let (letters, digits) = reference.split_at(split);

    if letters.is_empty()
        || digits.is_empty()
        || !letters.chars().all(|c| c.is_ascii_alphabetic())
        || !digits.chars().all(|c| c.is_ascii_digit())
    {
        return invalid(format!("invalid XLSX cell reference: {reference}"));
    }

    let mut column = 0usize;

    for character in letters.bytes() {
        let digit = usize::from(character.to_ascii_uppercase() - b'A' + 1);

        column = column
            .checked_mul(26)
            .and_then(|value| value.checked_add(digit))
            .ok_or_else(|| invalid_error("XLSX column reference overflow"))?;
    }

    let row = digits
        .parse::<u32>()
        .map_err(|_| invalid_error("invalid XLSX row reference"))?;

    if row == 0 {
        return invalid("XLSX row reference must be positive");
    }

    Ok((column - 1, row))
}

#[cfg(test)]
mod reference_tests {
    use super::*;

    #[test]
    fn parses_cell_references() {
        assert_eq!(parse_cell_reference("A1").unwrap(), (0, 1));
        assert_eq!(parse_cell_reference("B1").unwrap(), (1, 1));
        assert_eq!(parse_cell_reference("C1").unwrap(), (2, 1));

        assert_eq!(parse_cell_reference("A2").unwrap(), (0, 2));
        assert_eq!(parse_cell_reference("B2").unwrap(), (1, 2));
        assert_eq!(parse_cell_reference("C2").unwrap(), (2, 2));

        assert_eq!(parse_cell_reference("AA10").unwrap(), (26, 10));
    }
}

fn read_xml(reader: &mut impl Read, xml: &mut Vec<u8>, stage: &'static str) -> Result<()> {
    if let Some(limit) = limits::current() {
        *xml = crate::shared::io::bounded::read_bounded(reader, limit.workbook_xml_bytes, stage)
            .map_err(|e| match crate::shared::io::bounded::resource_error(&e) {
                Some(limit) => MarketForgeError::ResourceLimit(limit),
                None => invalid_error(format!("cannot read {stage}: {e}")),
            })?;
    } else {
        reader
            .read_to_end(xml)
            .map_err(|e| invalid_error(e.to_string()))?;
    }
    Ok(())
}
fn check_text(current: usize, additional: usize, stage: &'static str) -> Result<()> {
    let requested = current
        .checked_add(additional)
        .ok_or_else(|| invalid_error("XLSX text accounting overflow"))?;
    if let Some(limit) = limits::current() {
        limits::check("cell bytes", limit.record_bytes, requested as u64, stage)?;
    }
    Ok(())
}
fn check_retained_text(current: u64, additional: usize) -> Result<u64> {
    let requested = current
        .checked_add(additional as u64)
        .ok_or_else(|| invalid_error("XLSX retained text overflow"))?;
    if let Some(limit) = limits::current() {
        limits::check(
            "retained worksheet cell bytes",
            limit.workbook_xml_bytes,
            requested,
            "worksheet materialization",
        )?;
    }
    Ok(requested)
}
fn check_items(current: usize, stage: &'static str) -> Result<()> {
    let requested = (current as u64)
        .checked_add(1)
        .ok_or_else(|| invalid_error("XLSX item accounting overflow"))?;
    if let Some(limit) = limits::current() {
        let permitted = match stage {
            "worksheet cells" => 3,
            "shared strings" => limit
                .workbook_rows
                .checked_mul(3)
                .ok_or_else(|| invalid_error("shared string count overflow"))?,
            _ => limit.workbook_rows,
        };
        limits::check("workbook items", permitted, requested, stage)?;
    }
    Ok(())
}

#[cfg(test)]
mod resource_tests {
    use super::*;
    use std::io::Write;
    fn workbook(xml: &[u8]) -> Cursor<Vec<u8>> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        writer
            .start_file(
                "xl/worksheets/sheet1.xml",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        writer.write_all(xml).unwrap();
        writer.finish().unwrap()
    }
    #[test]
    fn expanded_xml_and_materialized_rows_are_bounded() {
        let xml = br#"<worksheet><sheetData><row r="1"><c r="A1"><v>1</v></c><c r="B1"><v>[[2,1]]</v></c><c r="C1"><v>[[1,1]]</v></c></row></sheetData></worksheet>"#;
        let mut limit = limits::NormalizationLimits::default();
        limit.workbook_xml_bytes = xml.len() as u64;
        {
            let _scope = limits::LimitScope::enter(limit, "tiny workbook".into());
            assert_eq!(XlsxDepthDecoder::decode(workbook(xml)).unwrap().len(), 1);
        }
        limit.workbook_xml_bytes -= 1;
        {
            let _scope = limits::LimitScope::enter(limit, "tiny workbook".into());
            assert!(
                matches!(XlsxDepthDecoder::decode(workbook(xml)), Err(MarketForgeError::ResourceLimit(ref e)) if e.stage == "worksheet XML")
            );
        }
        limit = limits::NormalizationLimits::default();
        limit.workbook_rows = 1;
        let _scope = limits::LimitScope::enter(limit, "rows synthetic".into());
        let rows = br#"<worksheet><row r="1"><c r="A1"><v>1</v></c><c r="B1"><v>2</v></c><c r="C1"><v>3</v></c></row><row r="2"><c r="A2"><v>1</v></c><c r="B2"><v>2</v></c><c r="C2"><v>3</v></c></row></worksheet>"#;
        // The cells themselves share the item limit; no oversized structure grows.
        assert!(matches!(
            parse_worksheet(rows, &[]),
            Err(MarketForgeError::ResourceLimit(_))
        ));
    }
}

#[cfg(test)]
mod observation_resource_tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn resource_failure_never_becomes_malformed_row_observation() {
        let mut limit = limits::NormalizationLimits::default();
        limit.depth_levels = 1;
        let _scope = limits::LimitScope::enter(limit, "task=9, source=tiny workbook".into());
        let xml = br#"<worksheet><row r="1"><c r="A1"><v>1</v></c><c r="B1"><v>[[2,1],[3,1]]</v></c><c r="C1"><v>[[1,1]]</v></c></row></worksheet>"#;
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        writer
            .start_file(
                "xl/worksheets/sheet1.xml",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        writer.write_all(xml).unwrap();
        let mut observations = 0;
        let result = XlsxDepthDecoder::decode_observed(writer.finish().unwrap(), |_, _, _| {
            observations += 1;
            Ok(())
        });
        assert!(
            matches!(result, Err(MarketForgeError::ResourceLimit(ref e)) if e.stage == "workbook level normalization" && e.scope.contains("task=9"))
        );
        assert_eq!(observations, 0);
    }
}
