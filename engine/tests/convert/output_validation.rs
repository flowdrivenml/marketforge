use std::path::PathBuf;
use std::time::Instant;

use marketforge_engine::process::convert::output::validate_converted_trades;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn validates_full_converted_dataset() {
    let directory = root().join("data/converted/multihop-USDT");

    let start = Instant::now();

    let metrics = validate_converted_trades(&directory, 5_000_000_000).unwrap();

    assert_eq!(metrics.events, 12_627_646);
    assert_eq!(metrics.identity, 11_435_942);
    assert_eq!(metrics.converted, 1_180_489);
    assert_eq!(metrics.unavailable, 11_215);

    assert_eq!(metrics.files, 1);
    assert_eq!(metrics.row_groups, 48);

    assert!(metrics.references_checked > 0);

    let elapsed = start.elapsed().as_secs_f64();

    println!();
    println!("{}", "=".repeat(80));
    println!("MARKETFORGE — PHYSICAL CONVERSION VALIDATION");
    println!("{}", "=".repeat(80));

    println!("Events             : {}", metrics.events);
    println!("Identity           : {}", metrics.identity);
    println!("Converted          : {}", metrics.converted);
    println!("Unavailable        : {}", metrics.unavailable);
    println!("References checked : {}", metrics.references_checked);
    println!("Files              : {}", metrics.files);
    println!("Row groups         : {}", metrics.row_groups);
    println!("Elapsed            : {:.3} s", elapsed);

    println!("STATUS             : PASS");
}
