# Troubleshoot conversion

Start with the [conversion run report](../workflows/inspection/runs.md), preparation report and any Engine diagnostics. Use the same environment as the original request.

| Symptom | What to inspect |
| --- | --- |
| Missing recipe name | Supply the positional `NAME` in `mf convert new NAME …`, or the recipe name or path for inspection and execution. |
| Invalid source publication | Use the positive numeric registry ID from the merged publication view. Check that the merge is committed, eligible and active. |
| Wrong environment | A direct recipe pins both the environment and registry UUID. Use the registry that owns the merge, or generate a new binding in the intended registry. |
| Missing or ambiguous merge | Review retained upstream lineage, publication eligibility and the candidate IDs. Produce missing upstream work separately, or pin the intended verified revision. Whole-publication approval covers a wider eligible input. |
| Missing reference trades or no route | Check stream IDs, supported layouts, historical base and quote currencies, external publication eligibility and the hop limit. A route needs an eligible trade reference for each leg. |
| Stale or missing event-level observation | Inspect the failure code and event times. A planned path can lack a fresh causal predecessor at a particular event. Review reference coverage before changing the staleness policy. |
| Invalid prepared job | Local recipe validation checks TOML and binding shape. Rust validates one standalone job and the physical inputs. Examine the retained compatibility blockers and re-prepare after correcting the cause. |
| Runtime incompatibility | Run `mf convert config schema` and `mf convert config validate`. The managed binary defines the schema. Compare the base runtime with the retained effective settings. |

## Managed converter and TUI

`mf engine status` identifies the installed converter. Use `mf engine build convert` to rebuild the managed target with its current interfaces. A prepared submission pins the managed executable, so prepare again after its checksum changes. Previously recorded runtime snapshots remain the evidence for those submissions.

A binary can support standalone Rust execution while lacking the managed TUI integration. Forge checks for `monitor-tui` and structured `--managed-output` handoff. Run `--tui` in an interactive terminal with stdin, stdout and stderr attached. If the monitor ran but Forge reports a handoff error, inspect the retained result and output before retrying. The screen alone cannot establish successful verification or indexing.

## Workspace ownership and interrupted publication

In the shell that launched the command, inspect `jobs -l`. Use `ps -p PID -o pid,ppid,stat,command` to check a known PID. A suspended process can still hold the workspace lock. Bring a suspended foreground job back with `fg`, then let it finish or interrupt it normally with Ctrl-C. Coordinate with the owner when another session holds the lock. Leave the lock file in place.

If physical publication appears to have finished, inspect `mf runs show RUN_ID --testing` and `mf runs report RUN_ID --testing`. Preserve the submitted job, pinned binary, execution intent and committed files. Rerunning the same compatible conversion lets Forge check for and recover matching committed work before materialization. A validated recovered commit can then be reused. A stale claim without provable committed output can block execution. Consult the [work identity and reuse rules](../concepts/conversion.md#execution-and-reuse) and [run index recovery](publications.md#recover-run-index-metadata). Run index recovery addresses history metadata.


## Automatic reference-selection failures

Open `prepare-report.json` from the CLI outcome or Runs. Review the source
publication, target quote, candidate eligibility and potential routes. When no
route exists, check the historical currency metadata and whether the
publication actually contains observed trades. Depth-only conversion needs
a permitted external trade-only publication with a usable route.

If several primary alternatives have missing or incomparable rankings, use
an applicable published turnover snapshot or select the exact trades
manually. An ex-post ranking override explicitly uses analytical evidence
that may be later than the source interval. If Rust validation passes but a
valuation is unavailable, inspect causal timestamps and per-event freshness.
