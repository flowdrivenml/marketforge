You are helping me inspect raw historical market-data fixtures for **MarketForge**.

The goal is to discover and document every distinct physical/raw data format before implementing normalization.

We will inspect **one exchange × instrument type × market category × data type at a time**.

Example:

```text
Exchange:        Gate.io
Instrument Type: Perpetual
Market Category: Linear
Data Type:       Order Book L2
```

## Goal

For every dataset determine:

- Compression/container format
- Internal file format and members
- Columns / fields
- Raw data types
- Timestamp representation, unit, and effective precision
- Timestamp ordering and archive time boundary
- Trade-side semantics
- Quantity semantics
- Order-book structure
- Snapshot vs delta/update behavior
- Sequence/update identifiers
- Integrity/continuity rules
- Exchange-specific fields
- Archive granularity and chunking
- Whether an existing raw parser can be reused

The final result should form a **raw-format/schema matrix** across all exchanges.

## Method

### 1. Use the Fixture as Ground Truth

Start from the actual file under:

```text
tests/fixtures/raw/
```

Do not assume the format from API documentation, filenames, another market, or another exchange.

If I provide the repository tree, use it to identify the exact fixture path.

Inspect only one dataset combination at a time.

### 2. Inspect Physical Structure First

Give me terminal commands to determine:

```text
container
→ members
→ internal format
→ header/schema
→ first records
→ last records
→ row/event count
```

Prefer standard terminal tools:

```text
file
gzip
unzip
tar
head
tail
awk
cut
sort
uniq
jq
```

For formats that cannot reasonably be inspected as text, such as XLSX, use a minimal Python script with the appropriate library.

Do not dump huge records when books contain hundreds or thousands of levels. Print compact summaries instead.

### 3. Inspect Semantics Second

After seeing the actual schema, generate a second targeted command.

For trades inspect where applicable:

```text
row count
trade ID uniqueness
trade ID continuity
first/last trade ID
timestamp ordering
timestamp precision
duplicate timestamps
side values
price validity
quantity validity
base/quote relationships
contract quantity semantics
exchange-specific fields
```

For order books inspect where applicable:

```text
event types
snapshot count
update/delta count
snapshot depth
level structure
bid/ask ordering
zero-quantity behavior
sequence IDs
update IDs
sequence continuity
snapshot frequency
timestamp ordering
duplicate timestamps
book reconstruction requirements
```

Only test properties that make sense for the observed format.

### 4. Never Guess Semantics

Do not assign meaning to an unknown field merely because it resembles another exchange.

For example, do not assume:

```text
1 = buy
2 = sell

positive quantity = bid

size = base quantity

archive date = UTC
```

First inspect the data.

If the raw file cannot establish the meaning, use official exchange documentation to verify it and clearly distinguish:

```text
Observed from fixture
Verified from documentation
Inferred / still unknown
```

### 5. Distinguish Representation From Semantics

Two markets can use the same physical parser while having different quantity semantics.

Example:

```text
same CSV schema
    ↓
same raw parser

Spot size
    → base quantity

Derivative size
    → contract quantity
```

Do not create a new parser merely because normalization semantics differ.

Create a new raw format ID only when the physical representation or required parsing logic materially differs.

Examples:

```text
OKX-T1
OKX-B1
OKX-B2

BITGET-T1
BITGET-B1

GATE-T1
GATE-T2
GATE-B1
GATE-B2
```

### 6. Inspect Archive Packaging

Archive organization is part of the raw format.

Detect patterns such as:

```text
one file per day

one file per month

one file per hour

one logical day split into:
_001
_002
_003

one option-chain archive containing
hundreds of contract members

ZIP
└── XLSX

TAR.GZ
└── JSONL

GZIP
└── headerless CSV
```

Document whether multiple physical files belong to one logical dataset period.

### 7. Treat Timestamps Carefully

Determine separately:

```text
storage representation
unit
precision
effective precision
ordering
archive boundary
```

For example:

```text
13-digit Unix milliseconds
but every value divisible by 1000
→ stored as ms
→ effective precision = 1 second
```

Always inspect actual first/last timestamps.

Do not assume the archive filename's date is UTC.

Check for boundaries such as:

```text
UTC
UTC+8
calendar month
hourly UTC
```

Canonical normalization dates should eventually come from record timestamps, not blindly from filenames.

### 8. Treat Order Books Carefully

Determine whether the dataset represents:

```text
full independent snapshots

snapshot + deltas

snapshot + updates

row-oriented snapshot followed by updates
```

Do not treat standalone snapshots as deltas.

For incremental books determine:

```text
initialization procedure
insert/update/delete semantics
sequence validation
reset behavior
```

If sequence information exists, explicitly derive and test its continuity rule.

Example:

```text
next.begin_id =
    current.begin_id + current.merged_count
```

### 9. Preserve Raw Anomalies

Do not silently clean unexpected behavior during inspection.

Record observations such as:

```text
out-of-order timestamps
duplicate timestamps
ID gaps
repeated IDs
missing fields
variable depth
partial-day coverage
multiple snapshots
non-monotonic rows
```

These observations will later determine validation and normalization behavior.

### 10. Keep Commands Efficient

Large fixtures may contain millions of records.

Prefer streaming inspection:

```text
gzip -cd ... | awk ...
unzip -p ... | jq ...
tar -xOzf ...
```

Avoid extracting huge archives unless required.

Do not repeatedly scan a massive file when one combined streaming pass can calculate all required statistics.

Keep command output compact enough that I can paste it back into the conversation.

## Workflow

For each dataset, follow this loop:

```text
1. I give you:
   exchange / market / data type

2. You identify the fixture.

3. You give me a compact structural inspection command.

4. I paste the output.

5. You interpret it.

6. If necessary, you give me one targeted semantic/integrity command.

7. I paste the output.

8. You create the final raw-format note.

9. Assign or reuse a parser ID.

10. Move to the next dataset.
```

Do not write the final format note until the important unknowns have been resolved.

## Final Note Format

Once inspection is complete, produce a concise Markdown note using this structure:

```markdown
## FORMAT-ID — Dataset Name

### Identity

| Field | Value |
|---|---|
| Exchange | |
| Instrument Type | |
| Market Category | |
| Data Type | |
| Format ID | |
| Fixture | |

### Physical Format

| Field | Value |
|---|---|
| Archive | |
| Internal Format | |
| Granularity | |

### Schema

| Field | Type | Meaning |
|---|---|---|

### Time

| Field | Value |
|---|---|
| Timestamp | |
| Unit | |
| Precision | |
| Ordering | |
| Archive Boundary | |

### Semantics

Document:

- side
- price
- quantity
- trade IDs
- contract semantics
- exchange-specific fields

### Order Book

For L2 only, document:

- snapshot/update model
- depth
- level representation
- insert/update/delete behavior
- sequence IDs
- reconstruction procedure

For trades:

```text
N/A
```

### Example

Include one small representative raw record and its interpretation.

### Notes

Record only important observed properties and anomalies.

### Compatibility

State whether this requires a new parser or reuses an existing parser.

**Parser:** `FORMAT-ID`  
**Ready for normalization:** Yes / No
```

## Core Rule

The inspection process is:

```text
observe
→ measure
→ verify
→ document
→ classify
```

Not:

```text
assume
→ design parser
→ force raw data into assumption
```

The raw fixture is the primary source of truth for physical representation. Official exchange documentation is used to resolve semantics that the raw bytes alone cannot establish.