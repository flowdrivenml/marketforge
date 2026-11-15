# Reference

Exact command references are available for [acquisition, external imports and managed results](cli/acquisition.md) and [markets, statistics and rankings](cli/markets.md). They distinguish inspection from network, file and registry mutations.

> **Documentation draft.** Other platform commands, processing contracts and data formats remain incomplete.

## Command sample

The Forge parser defines these help entry points:

```bash
marketforge --help
marketforge acquisition --help
marketforge normalize --help
```

`mf` is an alias installed by the same Python package. Global options include `--project-root`, `--config`, and `--json`. The current normalization group has `new`, `validate`, and `prepare`; preparation is not Engine execution. This sample was established by source inspection, not a runtime test.

## Contracts and outputs

Future reference pages will explain recipe fields, prepared job/batch versions, supported format profiles, canonical data representation, manifests, and diagnostic outputs. Version labels must be tied to their actual producer and consumer; matching version numbers alone do not prove compatibility.

Existing technical records remain outside this site's source tree in `docs/historical/`, `engine/docs/`, and module-local documentation. Consult them in the checkout with attention to their scope and currency; this scaffold does not certify those records against every implementation.
