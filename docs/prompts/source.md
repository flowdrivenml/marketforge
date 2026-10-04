```markdown
## MarketForge Exchange Source Implementation Prompt

Implement the MarketForge acquisition source for **{EXCHANGE}**, using `BybitSource` as the reference architecture.

Do not redesign the common models or `Source` interface unless the exchange exposes something that genuinely cannot be represented by them.

### Source Contract

Implement:

```python
supports(...)
instruments(...)
base_coins(...)      # only when meaningful
availability(...)
discover_files(...)
```

The source must return common MarketForge models:

```python
Instrument
BaseCoin
Availability
RemoteFile
```

Use:

```python
InstrumentType:
    SPOT
    PERPETUAL
    FUTURE
    OPTION

MarketCategory:
    SPOT
    LINEAR
    INVERSE
    OPTION

DataType:
    TRADE_TICKS
    ORDER_BOOK_L2
```

### Capabilities

Define an exchange-specific capability matrix:

```python
{EXCHANGE}_CAPABILITIES = {
    (
        InstrumentType....,
        MarketCategory....,
        DataType....,
    ),
}
```

Implement:

```python
supports(
    instrument_type,
    market_category,
    data_type,
) -> bool
```

Unsupported combinations must be rejected before unnecessary network requests.

### Instrument Discovery

`instruments()` must return the instruments actually exposed by the exchange and map exchange-specific terminology into MarketForge's canonical models.

Support filtering by:

```text
InstrumentType
MarketCategory
DataType
```

Where possible, derive `InstrumentType` from real exchange metadata rather than inferring it from the symbol.

If the exchange organizes some datasets by underlying/base coin rather than individual instruments, implement `base_coins()`.

### Historical Availability

`availability()` answers:

> What historical files actually exist?

There are two possible strategies.

```text
DIRECT
→ archive/listing exposes complete availability
→ parse once

SCAN
→ availability requires repeated API calls / probes
→ split into valid request windows
→ throttle requests
→ collect + deduplicate results
```

For expensive scans, use an exchange-specific `RequestPolicy`.

The exchange source defines the default policy; `HttpClient` enforces it.

Keep policies configurable so the CLI can override them later.

Availability should return:

```python
Availability(
    exchange=...,
    target=...,
    data_type=...,
    files=(...),
)
```

Files must be chronologically sorted.

Do not silently fill historical gaps.

### File Discovery

`discover_files()` answers:

> Which exact remote files are needed for this requested interval?

Signature:

```python
discover_files(
    target: Instrument | BaseCoin,
    data_type: DataType,
    start: datetime,
    end: datetime,
) -> list[RemoteFile]
```

Return standardized `RemoteFile` objects containing at least:

```text
exchange
instrument_type
market_category
data_type
symbol
start
end
url
filename
size_bytes when available
```

Use `[start, end)` internally.

`discover_files()` performs discovery only. It must **not download file contents**.

### HTTP

All networking goes through:

```python
HttpClient
```

The source determines:

```text
endpoint
HTTP method
parameters
payload
exchange-specific headers
RequestPolicy
```

`HttpClient` handles execution/throttling.

Do not scatter `requests.get/post/head()` directly through exchange modules.

### Parsing

Keep exchange-specific parsing in private helpers:

```python
_parse_...
_extract_...
_discover_...
```

Separate:

```text
raw exchange response
        ↓
exchange-specific parser
        ↓
MarketForge model
```

Do not leak exchange-specific response structures outside the source adapter.

---

## Tests

Create live tests for all implemented capabilities.

### Instrument Discovery

Test the complete valid parameter grid:

```python
InstrumentType × MarketCategory × DataType
```

For every supported combination verify:

```text
non-empty result
correct exchange
correct InstrumentType
correct MarketCategory
valid symbols
```

Print:

```text
Instrument Type
Market Category
Data Type
Total instruments
Sample symbols
```

Also test `base_coins()` where applicable.

### Availability

Test representative instruments for every supported category/data type.

Print:

```text
symbol/base coin
instrument type
market category
data type
file count
first available timestamp
last available timestamp
first files
last files
```

Verify:

```text
files exist
chronological ordering
correct metadata
correct intervals
```

For daily archives, inspect gaps between consecutive files:

```python
if current.start > previous.end:
    # historical gap
```

Print gaps for inspection but do not automatically fail simply because the exchange itself has missing historical data.

For scanned availability, initially test a small request window before testing long iterative scans.

### File Discovery

Test representative requested ranges for every supported combination.

Print:

```text
requested range
number of files
date
filename
download URL
```

Verify:

```text
files are chronologically ordered
all files overlap requested [start, end)
URLs exist in RemoteFile
filenames exist
metadata matches request
```

Also test:

```text
invalid start/end range
unsupported capability combinations
empty historical ranges where applicable
```

### Test Separation

Use:

```text
tests/unit/
    pure parsing, capability matrices, date/window logic

tests/fixtures/
    saved HTML/JSON/sample exchange responses

tests/integration/
    multi-component offline workflows

tests/live/
    actual exchange endpoints
```

Live tests should be verbose and useful for manual inspection:

```bash
python -m pytest tests/live/test_{exchange}_instruments.py -v -s

python -m pytest tests/live/test_{exchange}_availability.py -v -s

python -m pytest tests/live/test_{exchange}_discover_files.py -v -s
```

### Implementation Goal

The completed source should provide this pipeline:

```text
supports()
    ↓
Can MarketForge acquire this combination?

instruments() / base_coins()
    ↓
What can the user select?

availability()
    ↓
What historical data actually exists?

discover_files()
    ↓
What exact files satisfy the requested interval?

RemoteFile[]
    ↓
download.py
```

Keep the exchange adapter focused strictly on **discovery and translation into common MarketForge models**. Downloading, storage, normalization, Rust processing, merging, and quality analysis belong to later layers.
````
