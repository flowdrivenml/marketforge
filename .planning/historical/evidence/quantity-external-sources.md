# Additional quantity-source review

Checked 2026-10-30. Primary repository evidence is the actual database snapshot,
raw-format inspection documents, existing jobs and bounded archive observations.
Trade and book profiles were inspected separately; no book unit was inferred
solely from a trade endpoint.

- [Bybit order-book API](https://bybit-exchange.github.io/docs/v5/market/orderbook)
  confirms bid/ask arrays, matching-engine `cts`, update ID and cross-sequence
  semantics. It calls the second element “size” without explicitly establishing
  inverse denomination; it is not independent proof of the quantity unit.
- [Bitget merge-depth API](https://www.bitget.com/api-doc/classic/contract/market/Get-Merge-Depth)
  identifies `[price, quantity]` elements, but does not establish inverse archive
  quantity denomination. It does not justify enabling the inverse profile.
- [OKX API guide](https://www.okx.com/docs-v5) documents contract-valued derivative
  sizes and base-valued Spot quantities. Archive-specific schema/notes remain the
  evidence for the Historical book representation.

Expected values in bounded-depth-observations.json are independent decimal
calculations under the captured authoritative metadata. They are observations
and formula witnesses, not claims that all raw archive records were normalized.
Values named `*_decimal50` use Python Decimal precision 50 solely to expose a
repeating quotient; Rust retains its established finite Decimal semantics.

Limitations: two Bitget inner XLSX workbooks exceed the 4 MiB inspection bound
(13.8/17.0 MB uncompressed) and were inspected only at outer ZIP metadata level.
Two OKX future TAR files had a different first member from the explicitly requested
member; inspection stopped rather than scanning a large archive to find it. Those
four real depth samples remain unverified. Their tiny synthetic production-worker
legacy/TOML equivalence tests passed. No quantity or canonical-schema change was
made on the basis of incomplete observations.
