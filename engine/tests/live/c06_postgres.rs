//! Explicit opt-in real PostgreSQL witness. Uses libpq-managed authentication,
//! hard-coded isolated database/role, and a server-side identity guard per call.
//! psql is test infrastructure, never the production recording adapter.
use marketforge_engine::live::model::identity::StreamId;
use marketforge_engine::live::recording::schema::{StoredEvent, StreamKind, StreamRow};
use std::{
    io::Write,
    process::{Command, Stdio},
};
const GUARD: &str = "DO $$ BEGIN IF current_database() <> 'marketforge_live_test' OR current_user <> 'marketforge_live_test' OR session_user <> 'marketforge_live_test' THEN RAISE EXCEPTION 'test identity mismatch'; END IF; IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname=current_user AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)) THEN RAISE EXCEPTION 'test role is not restricted'; END IF; END $$;";
fn sql(query: &str) -> Result<String, String> {
    let mut child = Command::new("psql")
        .args([
            "--no-password",
            "--no-psqlrc",
            "--host=127.0.0.1",
            "--port=5432",
            "--username=marketforge_live_test",
            "--dbname=marketforge_live_test",
            "--set=ON_ERROR_STOP=1",
            "--quiet",
            "--tuples-only",
            "--no-align",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| e.to_string())?;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(format!("{GUARD}\n{query}\n").as_bytes())
        .map_err(|e| e.to_string())?;
    let output = child.wait_with_output().map_err(|e| e.to_string())?;
    if output.status.success() {
        Ok(String::from_utf8(output.stdout).unwrap())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).into_owned())
    }
}
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
fn insert(row: &StoredEvent, stream: &str) -> String {
    let e = &row.event;
    let record = quote(&serde_json::to_string(&e.event_record).unwrap());
    let mut query = format!(
        "INSERT INTO live_canonical.event_identities(stream_id,log_epoch,event_offset) VALUES ({stream},{},{}); INSERT INTO live_canonical.events(stream_id,log_epoch,event_offset,record_version,payload_kind,exchange_timestamp_ns,received_timestamp_ns,book_generation,event_record) VALUES ({stream},{},{},{},{},{},{},{},{record}::jsonb);",
        e.cursor.log_epoch.0,
        e.cursor.offset,
        e.cursor.log_epoch.0,
        e.cursor.offset,
        e.record_version,
        quote(&e.payload_kind),
        e.exchange_timestamp_ns
            .map(|v| v.to_string())
            .unwrap_or("NULL".into()),
        e.received_timestamp_ns,
        e.book_generation
            .map(|v| v.to_string())
            .unwrap_or("NULL".into())
    );
    for l in &row.levels {
        let numeric = |v: Option<marketforge_engine::live::model::codec::ExactDecimal>| {
            v.map(|v| v.0.to_string()).unwrap_or("NULL".into())
        };
        query += &format!(
            "INSERT INTO live_canonical.l2_levels(stream_id,log_epoch,event_offset,level_ordinal,side,action,price,quantity_base,quantity_quote,quantity_contracts,order_count) VALUES ({stream},{},{},{},{},{},{},{},{},{},{});",
            l.cursor.log_epoch.0,
            l.cursor.offset,
            l.level_ordinal,
            quote(&l.side),
            quote(&l.action),
            l.price.0,
            numeric(l.quantity_base.clone()),
            numeric(l.quantity_quote.clone()),
            numeric(l.quantity_contracts.clone()),
            l.order_count
                .map(|v| v.to_string())
                .unwrap_or("NULL".into())
        );
    }
    query
}
struct Fixture(String);
impl Drop for Fixture {
    fn drop(&mut self) {
        let stream = quote(&self.0);
        let cleanup = format!(
            "BEGIN; DELETE FROM live_canonical.recording_coverage WHERE stream_id={stream}; DELETE FROM live_canonical.events WHERE stream_id={stream}; DELETE FROM live_canonical.event_identities WHERE stream_id={stream}; DELETE FROM live_canonical.streams WHERE stream_id={stream}; COMMIT;"
        );
        if let Err(error) = sql(&cleanup) {
            eprintln!("isolated fixture cleanup failed: {error}");
        }
    }
}
#[test]
#[ignore = "requires explicitly authorized isolated PostgreSQL database and psql"]
fn real_group_commit_exact_readback_retry_conflict_and_atomic_rollback() {
    let id = format!("codex-c06-witness-{}", std::process::id());
    let mut fields = super::c06_recording::row(u64::MAX)
        .decode()
        .unwrap()
        .into_fields();
    fields.cursor.stream_id = StreamId(id.clone());
    fields.provenance.source_stream = StreamId(id.clone());
    let event = fields.try_into().unwrap();
    let row = StoredEvent::encode(
        &event,
        &StreamRow {
            stream_id: StreamId(id.clone()),
            instrument_id: None,
            stream_kind: StreamKind::L2,
        },
    )
    .unwrap();
    let stream = quote(&id);
    // Verify identity before creating the uniquely scoped fixture. Existing rows
    // are rejected, never reused or cleared based solely on a name prefix.
    sql(&format!(
        "INSERT INTO live_canonical.streams(stream_id,stream_kind) VALUES ({stream},'l2');"
    ))
    .unwrap();
    let _fixture = Fixture(id);
    sql(&format!("BEGIN; {1} INSERT INTO live_canonical.recording_coverage(stream_id,log_epoch,first_offset,last_offset) VALUES ({stream},1,{0},{0}); COMMIT;", u64::MAX, insert(&row,&stream))).unwrap();
    let raw = sql(&format!("SELECT jsonb_build_object('event',jsonb_build_object('cursor',jsonb_build_object('stream_id',e.stream_id,'log_epoch',e.log_epoch,'offset',e.event_offset),'record_version',e.record_version,'payload_kind',e.payload_kind,'exchange_timestamp_ns',e.exchange_timestamp_ns,'received_timestamp_ns',e.received_timestamp_ns,'book_generation',e.book_generation,'event_record',e.event_record),'levels',(SELECT jsonb_agg(jsonb_build_object('cursor',jsonb_build_object('stream_id',l.stream_id,'log_epoch',l.log_epoch,'offset',l.event_offset),'level_ordinal',l.level_ordinal,'side',l.side,'action',l.action,'price',l.price::text,'quantity_base',l.quantity_base::text,'quantity_quote',l.quantity_quote::text,'quantity_contracts',l.quantity_contracts::text,'order_count',l.order_count) ORDER BY l.level_ordinal) FROM live_canonical.l2_levels l WHERE l.stream_id=e.stream_id AND l.log_epoch=e.log_epoch AND l.event_offset=e.event_offset)) FROM live_canonical.events e WHERE e.stream_id={stream};")).unwrap();
    let decoded: StoredEvent = serde_json::from_str(raw.trim()).unwrap();
    assert_eq!(decoded, row);
    assert_eq!(decoded.decode().unwrap(), event);
    // A second session resolves an uncertain acknowledgement using the same
    // identity and full content. This simulates loss of the acknowledgement;
    // it does not claim a network-level commit interruption.
    assert_eq!(sql(&format!("SELECT count(*) FROM live_canonical.events WHERE stream_id={stream} AND event_offset={};",u64::MAX)).unwrap().trim(), "1");
    assert!(sql(&format!("BEGIN; {}; COMMIT;", insert(&row, &stream))).is_err());
    // Failing a child statement rolls back the previously inserted identity and
    // parent in this transaction. It cannot leave half an L2 group visible.
    let mut broken = row.clone();
    broken.event.cursor.offset = 0;
    for l in &mut broken.levels {
        l.cursor.offset = 0;
    }
    let statements = insert(&broken, &stream).replace("'bid'", "'invalid-side'");
    assert!(sql(&format!("BEGIN; {statements} COMMIT;")).is_err());
    assert_eq!(sql(&format!("SELECT count(*) FROM live_canonical.event_identities WHERE stream_id={stream} AND event_offset=0;")).unwrap().trim(), "0");
    assert_eq!(sql(&format!("SELECT count(*) FROM live_canonical.recording_coverage WHERE stream_id={stream} AND first_offset={0} AND last_offset={0};",u64::MAX)).unwrap().trim(), "1");
}
