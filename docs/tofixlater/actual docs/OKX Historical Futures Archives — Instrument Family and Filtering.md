**Observation:** OKX historical futures trade archives are organized by instrument family, not individual futures contracts.

The instrument family determines **which archive is downloaded**. The requested instrument symbol identifies the contract MarketForge intends to process, but does not restrict the contents of the downloaded archive.

### Download Behavior

MarketForge requires the correct instrument family when downloading OKX futures.

**Inverse futures:**

```bash
download_market \
  okx \
  future \
  inverse \
  BTC-USD-261225 \
  BTC-USD
```

**Linear futures:**

```bash
download_market \
  okx \
  future \
  linear \
  BTC-USD_UM-261225 \
  BTC-USD_UM
```

The final argument specifies the instrument family.

| Property | Inverse futures | Linear futures |
|---|---|---|
| Requested instrument | `BTC-USD-261225` | `BTC-USD_UM-261225` |
| Instrument family | `BTC-USD` | `BTC-USD_UM` |
| Archive prefix | `BTC-USD-futureschain` | `BTC-USD_UM-futureschain` |
| Contract value | 100 USD | 0.01 BTC |
| Settlement asset | BTC | USD |

**Important:** Supplying the wrong family downloads the wrong archive, even when the requested symbol and market category are correct.

For example, requesting `BTC-USD_UM-261225` with family `BTC-USD` downloads inverse futures-chain data rather than linear futures-chain data.

MarketForge currently relies on the supplied instrument family during historical discovery.

### Archive Contents

A single futures-chain archive contains trades from multiple contracts belonging to the same family.

Example:

```text
BTC-USD-futureschain-trades-2026-09-01.zip
```

The archive contains **12,938 trades across 7 instruments**:

| Instrument | Trades |
|---|---:|
| BTC-USD-260925 | 5,563 |
| BTC-USD-261030 | 2,685 |
| BTC-USD-261225 | 2,421 |
| BTC-USD-270326 | 1,447 |
| BTC-USD-260904 | 623 |
| BTC-USD-270625 | 196 |
| BTC-USD-270924 | 3 |

Each CSV record includes `instrument_name`, identifying the actual futures contract.

Consequently, downloading an archive for `BTC-USD-261225` does not mean every record belongs to that instrument.

### Processing Policy

MarketForge retains the existing acquisition workflow and filters instruments during Rust processing.

```text
OKX futures-chain archive
        ↓
Read instrument_name
        ↓
Compare with requested instrument
        │
        ├── Match → Normalize trade
        │
        └── Mismatch → Skip record
```

The canonical `instrument_id` and `symbol` must always correspond to the instrument identified by the source record.

**Implemented safeguards:**

- `IdentityPolicy::Filter` handles OKX futures-chain archives.
- Records belonging to unrelated instruments are skipped before normalization.
- Archives containing records but no matching instrument are rejected.
- Incorrect instrument attribution is prevented.
- Contract specifications are applied only to matching records.

### Verified Results

Real-archive integration tests confirmed filtering for both futures families.

**Inverse futures — `BTC-USD-261225`:**

```text
Archive: BTC-USD-futureschain-trades-2026-09-01.zip

Records inspected : 8,876
Records matched   : 5
Records skipped   : 8,871

Result: PASS
```

**Linear futures — `BTC-USD_UM-261225`:**

```text
Archive: BTC-USD_UM-futureschain-trades-2026-09-01.zip

Records inspected : 4,084
Records matched   : 5
Records skipped   : 4,079

Result: PASS
```

These statistics describe the records inspected until five matching trades were found, not the complete archive.

Both configurations successfully produced correctly attributed canonical trades with the appropriate contract calculations.

### Known Trade-off

Different futures contracts belonging to the same instrument family may download identical archives.

This introduces redundant downloads, storage, and processing.

**Decision:** Accept this redundancy to preserve the existing simple acquisition architecture.

Futures families contain relatively few contracts compared with options, so the additional overhead is considered acceptable.

Archive deduplication and shared family-level processing remain potential future optimizations, not current implementation requirements.

### Additional Observation — Incorrect Family Selection

During initial testing, the following downloaded archives had identical SHA-256 hashes:

```text
future/inverse/BTC-USD-261225/
future/linear/BTC-USD_UM-261225/
```

Both contained `BTC-USD-*` inverse futures records.

Investigation confirmed that the linear acquisition had used the incorrect instrument family, `BTC-USD`, instead of `BTC-USD_UM`.

The OKX API itself returned distinct, correct archive URLs for both families.

**Resolution:**

- Removed the incorrectly downloaded linear archives.
- Downloaded the correct `BTC-USD_UM-futureschain-*` archives.
- Updated the processing job to reference the corrected files.
- Verified instrument filtering and normalization against real archives.
- Confirmed all 18 trade configurations passed the integration test.

**Final rule:** For OKX futures, the instrument family controls archive selection, while `instrument_name` controls record-level filtering. Both must be correct to prevent cross-instrument data contamination.