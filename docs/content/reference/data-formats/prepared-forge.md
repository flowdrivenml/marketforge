# Forge preparation and run evidence

Forge reports coordinate the Engine handoff and recover committed output. Pass the operation's prepared job to its Engine, and use the separate retained evidence when working with Forge.

| Operation | Preparation | Execution and recovery |
| --- | --- | --- |
| Normalize | Batch schema 4 plus `prepare-report.json` with selection and job summaries | `submitted-batch.json`, `execution-intent.json`, Engine result and `execution-result.json`. Verified committed outputs can be registered from the supported receipt |
| Merge | `merge-job.json` and `prepare-report.json` | Pinned submitted job, configuration and selected inputs, execution intent and result. Registration recovery checks the same committed evidence |
| Convert | `convert.json` is a Forge object with `version: 1`, `type: "forge-conversion-collection"`, `environment`, `jobs`, `pins`, `sources`, `external_reference`, and `compatibility_blockers`. `jobs` indexes standalone job files and their validation state | Forge submits each job separately. The execution intent and result retain compatibility and output verification. Conversion registration is unavailable |
| Pipeline | Versioned recipe and read-only plan. Its child preparation artifacts use the corresponding operation schemas | `report.json` and `events.jsonl` under the run ID. Child stage records link to prepared inputs, execution attempts, verification and registration |

The pipeline's top-level report has `schema_version: 1`, `kind: "historical_pipeline"` and a `state`. Stage keys are `acquire`, `normalize`, `merge` and optional `convert`. Window dispatch outcomes are retained under the parent attempt’s `stage-windows/` directory. Each stage tracks phase and state, and processing stages may include prepared job and report pins, execution attempts, result and registered `primary_file_ids`. Status is a read of that report. Resume checks original input bindings and work context. It may recover a verified registration or allocate fresh output identities for a processing retry. Acquisition, preparation, submission and publication are separate transitions. [Pipeline service](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/pipelines/service.py) · [Resume guide](../../workflows/pipeline.md)

All artifact paths resolve according to their producer and contract. Treat retained inputs as immutable evidence. A preparation report can exist even if the job is blocked or generated without validation. Verify current binary compatibility before using older retained artifacts.


For automatic Conversion, each job entry in the Forge collection carries its
reference-resolution evidence. The preparation report shows the source and
external publication identities, stream eligibility, potential routes, the
selection rationale, and the Rust validation outcome. If a published ranking
was consulted, the evidence includes the snapshot identity, compared entries,
temporal applicability and fingerprint. Execution and resume use the exact
retained job and collection fingerprints, so changes to the rankings library
cannot change their reference bindings.
