# Glossary

| Term | Definition |
| --- | --- |
| Acceptance, rejection, exclusion | Decisions about source records or observations under the applicable interpretation and integrity rules. See [count populations](depth.md#read-counts-by-population) |
| Acquisition | Discovery and managed retrieval or registration of raw source data |
| Archive / source object | Addressable provider object with a nominal scope; may contain several members |
| Attempt / child run | One attempt under a logical run, or a separate linked stage operation. See [runs](execution.md#read-runs-and-attempts) |
| Authority / namespace | The instrument authority and identity scope used to interpret a canonical instrument ID |
| Base, quote, settlement, contract denomination | Economic units explained in [instrument references and fields](identity.md#instrument-references-and-economic-fields) |
| Batch / collection / job / task | Managed grouped work, exact Engine request, or a normalization input unit. See [the data model](data-model.md#from-an-input-selection-to-a-job) |
| Canonical row | Standardized physical trade or depth representation. One depth observation can span many normalized rows |
| Causal order | Observation order needed to preserve dependencies, potentially with regressing event timestamps |
| Checkpoint | Retained state associated with a boundary, used under the applicable reconstruction or conversion reader contract |
| Commit | Transition that exposes a finished Engine publication under its publication contract |
| Conversion | Historical valuation of preserved source events in a target quote using eligible reference trades |
| Coverage | Evidence about source availability, time extents or continuity for a stated population. See [time and coverage](time.md) |
| Depth event / level | A book observation / one side-price entry within a book or observation |
| Discovery and fetch times | Times Forge checked a source or retrieved bytes or metadata |
| Engine and registry IDs | Separate job, Engine publication, registry publication and registered-file identities. See [inspection identifiers](identity.md#inspection-identifiers) |
| Event and system timestamps | Source event time and an optional additional source clock |
| Evidence / fingerprint | Retained proof or observation of a stated fact / digest of a defined byte object or canonical document |
| Executable fingerprint | Digest identifying the Engine binary used or considered for compatibility |
| Explicit validity / assumed applicability | Declared historical interval or policy-derived scope for how specification evidence is used |
| Family scope | Provider grouping that can contain multiple concrete instruments |
| Integrity | Processing-quality evidence under applicable checks and policies |
| Managed local raw copy | Registered local source bytes tied to their origin and content evidence |
| Manifest / sidecar | Publication inventory and contract metadata / supporting artifact such as an event index or checksum inventory |
| Market event / reconstruction row / physical row | In-scope market observation / context row used for replay / a written Parquet record. See [counts](depth.md#read-counts-by-population) |
| Materialization / staging | Writing physical output / the location or state before commit |
| Merge | Combining compatible normalized observations into a common physical timeline |
| Native symbol / instrument reference | Venue-specific symbol / command-specific qualified identity |
| Nominal interval / observed endpoints | Advertised source scope / measured inclusive extrema under the stated population |
| Normalization | Interpreting resolved raw records and producing canonical streams |
| Output reference | Durable converted work key used to inspect the indexed output |
| Partition / primary file / row group | Output planning or layout unit / physical data file / internal Parquet storage unit |
| Profile | Source layout and semantic interpretation for normalization |
| Provider / venue | Origin that distributes data / the execution market the recording describes |
| Publication | Committed Engine output with manifest and required physical inventory |
| Recipe / runtime | Editable selection and intent / operation policy and resource configuration |
| Reconstruction / segment | State replay from applicable context / a reconstructable chain under the feed's rules |
| Reference staleness | Event-time age compared with the configured nanosecond limit for a reference observation |
| Reference trade / update | Price-providing selected trade / its admission to conversion price state |
| Registration | Linking verified output to publication and file identities in the selected registry |
| Registry identity | Verified database UUID and environment required for exact references |
| Requested interval | Declared time scope interpreted by the specific operation |
| Retirement / supersession | Removal from active selection / recording a canonical replacement |
| Reuse / recovery | Return of a verified equivalent result / completion of a recoverable handoff from retained evidence |
| Run time / publication time | Orchestration timing / commit or registration timing where retained |
| Sequence continuity | Valid progression under a particular feed's source sequence rules |
| Source records read | Count of records consumed by the source reader, before output row expansion |
| Source observation / revision | Timed source evidence / identified version of the source object |
| Stream / stream rank / timeline sequence | Logical observation series / prepared priority and binding / causal physical order |
| Structural validation / semantic validation / physical verification | Local declaration checks / Engine-compatible request or evidence checks / committed-output checks |
| Trade | An execution observation whose quantities and side follow the interpreted source contract |
| Work identity | Versioned key for resolved processing semantics and compatibility evidence |
