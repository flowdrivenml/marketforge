# Evidence, integrity, and reproducibility

Evidence answers specific questions about an input, interpretation, submission or output. The strongest conclusion depends on which fields were recorded and verified.

| Evidence | Question it can answer |
| --- | --- |
| Source key, revision and local content fingerprint | Which provider object and exact local bytes were selected? |
| Profile and interpreted source bindings | How did the job decode fields, timestamps, quantities and sides? |
| Historical instrument/specification evidence | Which economic terms apply under the documented source or inference policy? |
| Requested settings and effective runtime | Which resource, integrity and output policies actually governed the job? |
| Prepared job, exact submission hash and executable fingerprint | Which request and Engine binary were used? |
| Membership, ranks and source spans | Which streams were selected, and which observations or rows contributed to an output? |
| Manifest, primary-file checksums and required sidecars | Which committed files and layout can be verified against the publication contract? |
| Registry association or durable work index | Which selected database identities and retained records refer to the result? |
| Resolved-work identity | Does a request match an indexed interpretation of the same operation under the applicable equivalence contract? |

A checksum gives byte identity for the hashed object. Historical completeness, correct interpretation and economic meaning require other evidence. Equal Parquet bytes can result from distinct builds or distinct processing contracts. A current or inferred specification can guide processing without independently verifying historical terms.

## Interpret each check at its own boundary

Structural validity concerns document shape and constraints. Semantic validation evaluates a prepared job under its Engine contract. Compatibility checks concern selected inputs, layouts and dependencies. Processing integrity records policy observations, rejections, exclusions and degradation where available. Physical verification checks committed evidence. Registration links the verified result to the selected registry. None of these checks by itself establishes complete historical source coverage.

Publication-level lineage records selected inputs. File-level contribution evidence is more specific when available. Read the lineage scope before concluding that a given source contributed to one physical file.

## Reproduce the result you need

To reproduce an interpretation, retain the same source revisions, specifications, profiles, accepted policies and ordering choices. Physical packaging also depends on the runtime, format, partition planning and writer settings. Byte identity is an independently checkable property of the resulting artifacts. A recipe name alone carries none of that evidence. A changed executable fingerprint can affect reuse even if the documented behavior appears similar.

See the [prepared artifact contracts](../reference/data-formats/prepared.md), [publication manifests](../reference/data-formats/publications.md), [specification evidence](../workflows/catalogue/specifications.md), and [dataset lineage](../workflows/inspection/datasets.md).
