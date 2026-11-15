//! Normalization-internal, one-shot decoded input. Family routing decodes an
//! archive once; concrete workers consume only their own already-decoded rows.
use crate::shared::{error::Result, io::container::SourceContainer};
use csv::ByteRecord;
use std::{cell::RefCell, io::Read, path::Path};
pub(crate) enum DecodedSource {
    #[cfg(test)]
    Csv {
        headers: Option<ByteRecord>,
        records: Vec<(u64, ByteRecord)>,
    },
    #[cfg(test)]
    Json(Vec<(u64, serde_json::Value)>),
    DiskCsv {
        headers: Option<ByteRecord>,
        spool: crate::historical::normalize::reconcile::spool::Spool,
    },
    DiskJson(crate::historical::normalize::reconcile::spool::Spool),
}
thread_local! { static INPUT: RefCell<Option<DecodedSource>> = const { RefCell::new(None) }; }
struct Guard(Option<DecodedSource>);
impl Drop for Guard {
    fn drop(&mut self) {
        INPUT.with(|v| {
            v.replace(self.0.take());
        });
    }
}
pub(crate) fn with_decoded<T>(input: DecodedSource, run: impl FnOnce() -> T) -> T {
    let _guard = Guard(INPUT.with(|v| v.replace(Some(input))));
    run()
}
pub(crate) fn with_source_reader<T>(
    path: impl AsRef<Path>,
    container: SourceContainer,
    member: Option<&str>,
    callback: impl FnOnce(&mut dyn Read) -> Result<T>,
) -> Result<T> {
    if INPUT.with(|v| v.borrow().is_some()) {
        callback(&mut std::io::empty())
    } else {
        crate::shared::io::source::with_validated_source_reader(path, container, member, callback)
    }
}
type CsvRows = Box<dyn Iterator<Item = std::result::Result<(u64, ByteRecord), csv::Error>>>;
pub(crate) fn take_csv() -> Option<(Option<ByteRecord>, CsvRows)> {
    INPUT.with(|v| -> Option<(Option<ByteRecord>, CsvRows)> {
        match v.take() {
            #[cfg(test)]
            Some(DecodedSource::Csv { headers, records }) => {
                Some((headers, Box::new(records.into_iter().map(Ok))))
            }
            Some(DecodedSource::DiskCsv { headers, spool }) => {
                let rows = match spool.consume::<(u64, Vec<Vec<u8>>)>() {
                    Ok(rows) => rows,
                    Err(e) => return Some((headers, Box::new(std::iter::once(Err(csv_error(e)))))),
                };
                Some((
                    headers,
                    Box::new(rows.map(|row| {
                        row.map(|(ordinal, fields)| (ordinal, ByteRecord::from(fields)))
                            .map_err(csv_error)
                    })),
                ))
            }
            other => {
                v.replace(other);
                None
            }
        }
    })
}
fn csv_error(error: crate::shared::error::MarketForgeError) -> csv::Error {
    let io = match error {
        crate::shared::error::MarketForgeError::ResourceLimit(e) => std::io::Error::other(e),
        e => std::io::Error::other(e),
    };
    io.into()
}
type JsonRows = Box<dyn Iterator<Item = Result<(u64, serde_json::Value)>>>;
pub(crate) fn take_json() -> Option<JsonRows> {
    INPUT.with(|v| -> Option<JsonRows> {
        match v.take() {
            #[cfg(test)]
            Some(DecodedSource::Json(records)) => Some(Box::new(records.into_iter().map(Ok))),
            Some(DecodedSource::DiskJson(spool)) => {
                match spool.consume::<(u64, serde_json::Value)>() {
                    Ok(rows) => Some(Box::new(rows)),
                    Err(e) => Some(Box::new(std::iter::once(Err(e)))),
                }
            }
            other => {
                v.replace(other);
                None
            }
        }
    })
}

pub(crate) fn ensure_routed(task: &crate::historical::job::WorkTask) -> Result<()> {
    if crate::historical::normalize::streams::is_family(task)
        && !INPUT.with(|v| v.borrow().is_some())
    {
        return Err(crate::shared::error::MarketForgeError::InvalidConfiguration("family selection requires prepared-batch stream routing; isolated concrete worker calls cannot establish the complete selected population".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scratch_io_errors_retain_their_engine_category_through_csv() {
        let job = crate::historical::config::selection_test_support::resolved(
            &crate::historical::config::selection_test_support::batch(),
        );
        let error = csv_error(
            crate::shared::error::MarketForgeError::InvalidConfiguration(
                "reconciliation scratch I/O: injected failure".into(),
            ),
        );
        let error = super::super::integrity::csv_source_error(&job.tasks[0], error);
        assert!(
            matches!(error,crate::shared::error::MarketForgeError::InvalidConfiguration(ref message) if message.contains("scratch I/O"))
        );
    }
    #[test]
    fn one_shot_inputs_preserve_physical_ordinals_without_reopening() {
        let input = DecodedSource::Csv {
            headers: Some(ByteRecord::from(vec!["symbol"])),
            records: vec![
                (2, ByteRecord::from(vec!["A"])),
                (9, ByteRecord::from(vec!["A"])),
            ],
        };
        with_decoded(input, || {
            with_source_reader(
                "nonexistent-synthetic-source",
                SourceContainer::Plain,
                None,
                |reader| {
                    let (_, rows) = take_csv().unwrap();
                    assert_eq!(rows.map(|r| r.unwrap().0).collect::<Vec<_>>(), [2, 9]);
                    assert_eq!(reader.read(&mut [0]).unwrap(), 0);
                    Ok(())
                },
            )
        })
        .unwrap();
        assert!(take_csv().is_none());
        let input = DecodedSource::Json(vec![(7, serde_json::json!({"instId":"A"}))]);
        with_decoded(input, || {
            let (ordinal, value) = take_json().unwrap().next().unwrap().unwrap();
            assert_eq!(ordinal, 7);
            assert_eq!(value["instId"], "A");
        });
        assert!(take_json().is_none());
    }
}
