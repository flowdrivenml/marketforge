use std::io::{Cursor, Read, Seek};

use quick_xml::{Reader, events::Event};
use serde_json::{Value, json};
use zip::ZipArchive;

use crate::{
    error::{MarketForgeError, Result},
    job::IntegrityCategory,
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
        let mut workbook = ZipArchive::new(reader)
            .map_err(|error| invalid_error(format!("cannot open XLSX workbook: {error}")))?;

        let shared_strings = read_shared_strings(&mut workbook)?;

        let sheet_name = find_worksheet(&mut workbook)?;

        let mut sheet = workbook
            .by_name(&sheet_name)
            .map_err(|error| invalid_error(format!("cannot open worksheet: {error}")))?;

        let mut xml = Vec::new();

        sheet
            .read_to_end(&mut xml)
            .map_err(|error| invalid_error(format!("cannot read worksheet: {error}")))?;

        let rows = parse_worksheet(&xml, &shared_strings)?;

        let mut records = Vec::new();

        for (ordinal, row) in rows.into_iter().enumerate() {
            if ordinal == 0 && row.first().map(String::as_str) == Some("timestamp") {
                continue;
            }

            let source_ordinal = u64::try_from(records.len())
                .map_err(|_| invalid_error("XLSX source ordinal overflow"))?
                + 1;

            if row.len() != 3 {
                return invalid(format!(
                    "XLSX row {source_ordinal}: expected 3 columns, received {}",
                    row.len()
                ));
            }

            let timestamp = row[0].parse::<i64>().map_err(|_| {
                invalid_error(format!("XLSX row {source_ordinal}: invalid timestamp"))
            })?;

            let asks = parse_levels(&row[1], source_ordinal, "asks")?;
            let bids = parse_levels(&row[2], source_ordinal, "bids")?;

            records.push(XlsxDepthRecord {
                source_ordinal,
                timestamp,
                record: json!({
                    "timestamp": timestamp.to_string(),
                    "asks": asks,
                    "bids": bids,
                }),
            });
        }

        records.sort_by_key(|record| (record.timestamp, record.source_ordinal));

        Ok(records)
    }
}

fn find_worksheet<R: Read + Seek>(workbook: &mut ZipArchive<R>) -> Result<String> {
    let sheets = workbook
        .file_names()
        .filter(|name| name.starts_with("xl/worksheets/sheet") && name.ends_with(".xml"))
        .map(str::to_owned)
        .collect::<Vec<_>>();

    if sheets.len() != 1 {
        return invalid(format!(
            "expected exactly one XLSX worksheet, found {}",
            sheets.len()
        ));
    }

    Ok(sheets[0].clone())
}

fn read_shared_strings<R: Read + Seek>(workbook: &mut ZipArchive<R>) -> Result<Vec<String>> {
    let Ok(mut file) = workbook.by_name("xl/sharedStrings.xml") else {
        return Ok(Vec::new());
    };

    let mut xml = Vec::new();

    file.read_to_end(&mut xml)
        .map_err(|error| invalid_error(format!("cannot read XLSX shared strings: {error}")))?;

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
            Event::Start(event) if event.name().as_ref() == b"si" => {
                current.clear();
                inside_string = true;
            }

            Event::Start(event) if event.name().as_ref() == b"t" => {
                inside_text = true;
            }

            Event::Text(event) if inside_string && inside_text => {
                let text = event
                    .xml_content()
                    .map_err(|error| invalid_error(error.to_string()))?;

                let text = quick_xml::escape::unescape(&text)
                    .map_err(|error| invalid_error(error.to_string()))?;

                current.push_str(&text);
            }

            Event::End(event) if event.name().as_ref() == b"t" => {
                inside_text = false;
            }

            Event::End(event) if event.name().as_ref() == b"si" => {
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
            Event::Start(event) if event.name().as_ref() == b"row" => {
                if inside_row {
                    return invalid("nested XLSX worksheet rows");
                }

                current_row.clear();
                current_row_number = None;

                for attribute in event.attributes() {
                    let attribute = attribute.map_err(|error| invalid_error(error.to_string()))?;

                    if attribute.key.as_ref() == b"r" {
                        let text = std::str::from_utf8(attribute.value.as_ref())
                            .map_err(|_| invalid_error("invalid XLSX row reference encoding"))?;

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

            Event::Start(event) if event.name().as_ref() == b"c" => {
                if !inside_row || inside_cell {
                    return invalid("invalid XLSX cell structure");
                }

                current_value.clear();
                current_type.clear();
                current_column = None;

                for attribute in event.attributes() {
                    let attribute = attribute.map_err(|error| invalid_error(error.to_string()))?;

                    match attribute.key.as_ref() {
                        b"r" => {
                            let reference =
                                std::str::from_utf8(attribute.value.as_ref()).map_err(|_| {
                                    invalid_error("invalid XLSX cell reference encoding")
                                })?;
                            let (column, row_number) = parse_cell_reference(reference)?;

                            if Some(row_number) != current_row_number {
                                return invalid(format!(
                                    "XLSX cell {reference} does not belong to worksheet row {:?}",
                                    current_row_number
                                ));
                            }

                            current_column = Some(column);
                        }

                        b"t" => {
                            current_type =
                                String::from_utf8_lossy(attribute.value.as_ref()).into_owned();
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
                    && (event.name().as_ref() == b"v" || event.name().as_ref() == b"t") =>
            {
                inside_value = true;
            }

            Event::Text(event) if inside_value => {
                let text = event
                    .xml_content()
                    .map_err(|error| invalid_error(error.to_string()))?;

                let text = quick_xml::escape::unescape(&text)
                    .map_err(|error| invalid_error(error.to_string()))?;

                current_value.push_str(&text);
            }

            Event::End(event) if event.name().as_ref() == b"v" || event.name().as_ref() == b"t" => {
                inside_value = false;
            }

            Event::End(event) if event.name().as_ref() == b"c" => {
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

                    shared_strings
                        .get(index)
                        .ok_or_else(|| invalid_error("XLSX shared-string index out of range"))?
                        .clone()
                } else {
                    std::mem::take(&mut current_value)
                };

                if current_row.insert(column, value).is_some() {
                    return invalid(format!("duplicate XLSX cell in column {column}",));
                }

                inside_cell = false;
            }

            Event::End(event) if event.name().as_ref() == b"row" => {
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
