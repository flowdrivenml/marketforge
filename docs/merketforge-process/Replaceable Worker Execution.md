The initial worker implementation remains deliberately simple:

```text
WorkTask
    ↓
SequentialTaskRunner
    ↓
Open Source
    ↓
Decompress / Unpack
    ↓
Parse
    ↓
Normalize
    ↓
Validate
    ↓
Bounded Batch
    ↓
Sort if Required
    ↓
Temporary Run
    ↓
TaskResult
```

Parallelism initially exists **between workers**, while each individual task is processed sequentially.

The worker stages remain separate reusable components:

```text
SourceReader
Parser
Normalizer
Validator
BatchBuffer
Sorter
RunWriter
```

Exchange-specific processors define market-data semantics but do not control threading, scheduling, or concurrency.

The execution boundary remains:

```text
WorkTask
    ↓
TaskRunner
    ↓
TaskResult
```

This allows the execution strategy to be replaced later:

```text
SequentialTaskRunner     ← initial
ParallelTaskRunner       ← possible optimization
```

without rewriting parsers, normalization, validation, canonical schemas, or format-specific logic.

> Freeze processing semantics; keep execution strategy replaceable.