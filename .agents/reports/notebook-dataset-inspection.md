# Python dataset discovery and notebook inspection

The Python catalog now returns physical file handles and uses file and stream metadata for composable filters. Opening a publication directory remains available as a labeled grouping. Filesystem discovery requires no registry connection. Callers can supply existing read-only registry summaries to attach IDs and file memberships.

Normalized inventory uses primary manifest entries and checks footer evidence, including outward-truncated byte-array statistics. Merged inventory uses the manifest stream definitions and file-specific registered membership or proven footer ranks. Rank ranges cannot prove all intermediate ranks. Selected stream reads apply rank and combined event-kind selectors. Unknown or conflicting identity is visible in discovery diagnostics.

Inspection caches Arrow schemas and footer statistics for rich displays. Explicit previews retain Decimal256 as exact text, Arrow iterators retain native exact values, and projected Polars queries stay lazy. Profiles use a bounded physical prefix and report selected rows independently. No inspection certifies continuity or applies price or quantity scaling.

Verification covered 43 focused Python tests, Ruff, Pyright and the Zensical build. All five notebook code cells executed on temporary local fixtures. The catalog and inspection HTML were reviewed using Chrome at 1280px in a light theme and 420px in a dark theme. The repository's committed-output tree returned 104 files in a metadata-only check, with one existing merged venue conflict reported. No processing workloads or large event scans ran.
