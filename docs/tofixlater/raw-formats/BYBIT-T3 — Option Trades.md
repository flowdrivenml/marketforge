### Identity

| Field | Value |
|---|---|
| Exchange | Bybit |
| Instrument Type | Option |
| Market Category | Option |
| Data Type | Trade ticks |
| Format ID | `BYBIT-T3` |
| Fixture | `2026-09-01_BTC_USDT.trades.csv.zip` |

### Physical Format

| Field | Value |
|---|---|
| Archive | ZIP / deflate |
| Internal Format | CSV |
| Granularity | Daily |

### Schema

| Column | Type | Meaning |
|---|---|---|
| `trade_id` | UUID/string | Unique trade ID |
| `trade_seq` | integer | Cross sequence |
| `timestamp` | integer | Execution timestamp, Unix ms |
| `instrument_name` | string | Option instrument |
| `direction` | string | Taker side: `Buy` / `Sell` |
| `price` | decimal | Execution price |
| `amount` | decimal | Executed option size |
| `iv` | decimal | Trade implied volatility |
| `index_price` | decimal | Index price at execution |
| `mark_price` | decimal | Option mark price at execution |
| `mark_iv` | decimal | Mark-price implied volatility |

### Time

| Field | Value |
|---|---|
| Timestamp Column | `timestamp` |
| Representation / Unit | Unix epoch milliseconds |
| Precision | Millisecond |
| Ordering | Chronological |

### Semantics

| Field | Value |
|---|---|
| Price | `price` |
| Quantity | `amount` |
| Side | Taker side |
| Trade ID | `trade_id`, unique |
| Sequence | `trade_seq`, may be shared by multiple trades |
| Trade IV | `iv` |
| Mark IV | `mark_iv` |
| Index Price | `index_price` |
| Mark Price | `mark_price` |

### Option Instrument

Example:

```text
BTC-1SEP26-78500-P-USDT
```

Observed components:

```text
BTC      base
1SEP26   expiry
78500    strike
P        put
USDT     quote/settlement
```

Both `C` and `P` contracts are present in the archive.

### Order Book

N/A

### Example

```text
trade_id,trade_seq,timestamp,instrument_name,direction,price,amount,iv,index_price,mark_price,mark_iv
398583df-bc95-58d6-ac59-3effd57c5c0d,74394749937,1788220801096,BTC-1SEP26-78500-P-USDT,Sell,300,0.04,0.3597,78581.70613787,294.89701743,0.3544
```

### Notes

- 54,274 records observed.
- 27,571 Buy / 26,703 Sell.
- 28,208 Call / 26,066 Put.
- All instruments use BTC base and USDT quote/settlement in this fixture.
- All trade IDs present and unique.
- `trade_seq` is not unique; 6,605 rows reuse a sequence value.
- No out-of-order timestamps observed.
- No missing values observed in price, amount, IV, index price, mark price, or mark IV.
- Structurally distinct from `BYBIT-T1` and `BYBIT-T2`.

### Compatibility

No previously inspected Bybit trade format is compatible.

**Parser:** `BYBIT-T3`  
**Ready for normalization:** Yes