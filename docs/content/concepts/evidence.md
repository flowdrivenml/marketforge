# Execution, publication, and lineage

Preparation resolves a recipe to concrete inputs and freezes the evidence needed for execution. Validation can reject a malformed or incompatible job before processing. A successful execution outcome can still be followed by a failed validation or registration step.

An Engine publication is committed on disk. Forge verifies that committed evidence before a separate registry transaction where registration is supported. A failed registry operation can leave valid files available for registration-only recovery. Inspect the operation's intent and outcome before starting another attempt. Conversion currently verifies completed output without registering it in Forge. [Operation lifecycles](../development/operations.md) · [Forge artifacts](../reference/prepared-forge.md)

Publication-level selection records the intended upstream inputs. An exact file-level contribution records evidence that a source contributed rows or events to that primary file. A dataset's integrity status summarizes the applicable processing and validation evidence. It does not guarantee continuous historical coverage or external source completeness. [Dataset lineage procedure](../workflows/datasets.md)
