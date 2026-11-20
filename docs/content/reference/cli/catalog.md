<!-- Suggested file: docs/content/reference/cli/catalog.md -->

# Market Catalogue

Before MarketForge can process historical market data, it needs to understand which financial instruments the records belong to and how their prices and quantities should be interpreted.

For example, imagine downloading BTCUSDT historical data from Bybit.

The symbol `BTCUSDT` alone does not tell us everything about the instrument. Bybit offers both spot trading and perpetual contracts using that symbol. Although their names are identical, they represent different markets.

The same problem becomes more complicated when working with futures, options, equities, or data purchased from external providers.

**MarketForge solves this by maintaining its own catalogue of exchanges, instruments, and their specifications.**

The catalogue allows MarketForge to identify instruments consistently, understand their economic properties, and associate historical recordings with the correct market.

This guide explains why the catalogue exists, how its information is organized, and how to register and inspect instruments manually.

---

## 1. Why Does MarketForge Need a Catalogue?

Different exchanges organize and describe their markets differently.

Some use symbols such as `BTCUSDT`, while others use `BTC-USDT-SWAP` or `BTC_USDT`.

Even when two exchanges use similar symbols, their instruments may have different trading rules.

For example:

- One exchange may express trade quantities directly in BTC.
- Another may express quantities in contracts.
- A perpetual contract may have a different contract value from an inverse perpetual.
- The minimum price increment may differ between instruments.
- Instrument specifications may change over time.

If MarketForge relied only on the information inside historical files, it could interpret some records incorrectly.

For example, consider a trade record containing:

```text
Price:    80000
Quantity: 100
```

What does `100` mean?

It could represent:

- 100 BTC.
- 100 derivative contracts.
- 100 USD of contract value.

The correct interpretation depends on the instrument and its specifications.

**MarketForge needs this information before it can safely normalize historical data.**

The catalogue provides a consistent place to store it.

### What does the catalogue contain?

The catalogue organizes three main types of information.

| Component | What it describes |
|---|---|
| **Exchange** | The trading venue where the instrument belongs. |
| **Instrument** | The specific financial product being traded. |
| **Specifications** | The economic rules and quantity units used to interpret that instrument. |

These records are connected.

```text
                Exchange
                   |
                   v
               Instrument
                   |
                   v
             Specifications
                   |
                   v
          Historical Market Data
```

For example:

```text
                  Bybit
                    |
                    v
                 BTCUSDT
              Linear Perpetual
                    |
                    v
               Specifications
                    |
                    v
             Historical Trades
              and Order Books
```

The exchange tells us where the instrument belongs.

The instrument tells us what is being traded.

The specifications tell us how to interpret the recorded values.

### Why not just use the exchange's API?

MarketForge can collect instrument information from supported exchanges.

However, not every historical dataset comes from an exchange with an available API.

For example, you might have:

- Historical data purchased from Databento.
- Recordings collected by your own infrastructure.
- Archives from an exchange that MarketForge does not support directly.
- Historical contracts that are no longer listed.
- Data exported from a proprietary trading system.

In these cases, MarketForge cannot always obtain the necessary information automatically.

**Manual catalogue registration allows you to provide the missing information yourself.**

This makes MarketForge useful beyond its built-in exchange integrations.

---

## 2. How Manual Catalogue Registration Works

Suppose you have a historical recording for a financial instrument that is not yet registered in MarketForge.

Before the recording can be used for normalization, MarketForge needs to know what the data represents.

You provide this information through small TOML files called **catalogue declarations**.

Each declaration describes one part of the market.

The registration process follows this order:

| Step | What you register | Why |
|---|---|---|
| 1 | Exchange | Establish where the instrument belongs. |
| 2 | Instrument | Identify the specific market and product. |
| 3 | Specifications | Describe how to interpret its prices and quantities. |

The order matters because an instrument must belong to an existing exchange, and specifications must refer to an existing instrument.

Once these declarations are registered, MarketForge can use them when associating raw files with the correct instrument.

### Catalogue declarations are not data files

A catalogue declaration does not contain trades or order-book updates.

It describes the market to which those records belong.

For example:

```text
exchange.toml
    |
    v
instrument.toml
    |
    v
specs.toml
    |
    v
Registered Catalogue
    |
    v
Raw Data Registration
    |
    v
Normalization
```

MarketForge stores the registered information in PostgreSQL.

The original TOML files remain useful because they describe exactly what you submitted.

### When should you register information manually?

Manual registration is particularly useful when working with external recordings or historical instruments whose metadata is not already available in the catalogue.

You do not need to repeat manual registration for an existing instrument whose catalogue information is already correct and sufficient for your intended workflow.

You can inspect the existing catalogue first to avoid unnecessary declarations.

---

## 3. Before You Begin

The catalogue commands use the MarketForge testing database.

Before registering anything, make sure Forge is installed, PostgreSQL is configured, and the database schema is up to date.

See [Installation](../../getting-started/installation.md) for the initial setup.

From the project root, run:

```bash
mf database migrate --testing
```

You can inspect the database afterward:

```bash
mf database status --testing
```

### What does `--testing` mean?

The current catalogue commands use the explicitly configured testing database for registration and inspection.

For example:

```bash
mf catalog exchange list --testing
```

**`--testing` does not mean the command is a dry run.**

Adding an exchange, instrument, or specification creates or reuses real records in the testing catalogue.

Validation is different: it checks a local declaration without connecting to PostgreSQL.

### Do I need the Rust Engine?

You do not need to compile the Rust normalization engine merely to validate or register exchange, instrument, and specification declarations.

These catalogue operations are handled by Forge.

However, later stages such as validating normalization profiles and processing historical data require the appropriate managed Rust Engine.

See [Engine management](engine.md).

### Example used in this guide

We will use the synthetic example included in the MarketForge repository.

The example describes:

| Property | Value |
|---|---|
| Exchange | Example Exchange |
| Exchange code | `example_exchange` |
| Instrument | AAPL |
| Instrument type | Equity |
| Market category | Equity |
| Base asset | AAPL |
| Quote asset | USD |
| Quantity unit | Shares |

**Example Exchange is fictional.**

Although the example uses the familiar AAPL symbol, its venue and economic specifications are demonstration values.

They are not claims about the actual Nasdaq market.

This allows us to explain catalogue registration without requiring live exchange access or historical market-data downloads.

The example declarations are located under:

```text
docs/content/examples/catalogue/
```

We will use these files throughout the guide.

---

## 4. Register an Exchange

The first step is to register the exchange or trading venue.

### Why do we register the exchange first?

Every instrument needs to belong to a specific market.

For example, BTCUSDT on Bybit is not the same instrument as BTCUSDT on Binance.

Even when symbols look identical, their trading rules, available data, and market histories may differ.

MarketForge therefore keeps exchange identity separate from instrument identity.

Before adding an instrument, we establish the venue to which it belongs.

### Create an exchange declaration

An exchange declaration is a small TOML file.

Our example contains:

```toml
code = "example_exchange"
name = "Example Exchange"
```

There are two fields:

| Field | Meaning |
|---|---|
| `code` | The identifier MarketForge uses to refer to the exchange. |
| `name` | The human-readable name of the exchange. |

The code is particularly important.

Other catalogue declarations use it to identify the venue.

For example, an instrument can refer to:

```toml
exchange = "example_exchange"
```

The exchange code must follow the catalogue's naming rules.

The documented convention uses lowercase letters, digits, and underscores.

Choose a stable code because other records will depend on it.

### Validate the exchange declaration

Before registering the exchange, check the TOML file:

```bash
mf catalog exchange validate \
    --file docs/content/examples/catalogue/exchange.toml
```

MarketForge reads the declaration and checks whether its structure and values are valid.

This command does not connect to PostgreSQL or change the catalogue.

It only validates the local file.

### Add the exchange to the catalogue

Once the declaration is valid, register it:

```bash
mf catalog exchange add \
    --from docs/content/examples/catalogue/exchange.toml \
    --testing
```

MarketForge validates the declaration and writes the exchange information to PostgreSQL.

The catalogue assigns the exchange its internal database identity.

You do not need to choose a numeric database ID yourself.

### List registered exchanges

To inspect the exchanges already stored in your testing catalogue:

```bash
mf catalog exchange list --testing
```

This is useful when you want to check whether a venue has already been registered.

### Inspect one exchange

To inspect Example Exchange:

```bash
mf catalog exchange show example_exchange --testing
```

The command looks up the exchange by its code.

In this example:

```text
example_exchange
```

is the exchange reference.

### What happens if the exchange already exists?

MarketForge supports repeated registration of the same declaration.

If the exchange code already exists with the same registered properties, the operation can reuse the existing entry.

However, if you attempt to register a different exchange name under the same code, MarketForge rejects the conflicting declaration.

This prevents one exchange identifier from silently changing meaning.

For example, you should not reuse `example_exchange` for an unrelated venue merely because the original entry is no longer needed.

Use a distinct code for a distinct exchange.

<!-- SCREENSHOT PLACEHOLDER: catalogue-exchange.png

Suggested image:
Terminal output showing:

mf catalog exchange add --from docs/content/examples/catalogue/exchange.toml --testing

Or:

mf catalog exchange show example_exchange --testing

Capture:
- Exchange code
- Exchange name
- Assigned database ID, if displayed
- Successful registration result

Purpose:
Show how a manually registered exchange appears in MarketForge.

Suggested alt text:
"MarketForge registering and inspecting a manually declared trading exchange."

-->

---

## 5. Register an Instrument

Now that Example Exchange exists, we can register an instrument belonging to it.

### What exactly is an instrument?

An instrument is a specific financial product traded on a particular market.

Examples include:

- BTCUSDT spot on Bybit.
- BTCUSDT linear perpetual on Bybit.
- BTCUSD inverse perpetual on Bybit.
- A dated BTC futures contract on OKX.
- AAPL shares traded on Nasdaq.

An instrument is more specific than a symbol.

For example, these two instruments may use the same native symbol:

```text
Bybit BTCUSDT Spot
Bybit BTCUSDT Linear Perpetual
```

They are not interchangeable.

One represents directly traded assets, while the other represents a perpetual derivative contract.

MarketForge therefore uses several properties together to identify an instrument.

### Understanding instrument identity

The catalogue identifies an instrument using:

| Property | Purpose |
|---|---|
| Exchange | Which venue the instrument belongs to. |
| Instrument type | Whether it is spot, perpetual, future, equity, or another supported type. |
| Market category | The market or contract category. |
| Symbol | The native name used for the instrument. |

For example:

```text
bybit.perpetual.linear.BTCUSDT
```

This reference means:

| Part | Value |
|---|---|
| Exchange | `bybit` |
| Instrument type | `perpetual` |
| Market category | `linear` |
| Symbol | `BTCUSDT` |

Compare it with:

```text
bybit.spot.spot.BTCUSDT
```

Both references contain BTCUSDT, but they identify different instruments.

This separation prevents historical records from being associated with the wrong market.

### Create an instrument declaration

Our synthetic AAPL instrument uses:

```toml
exchange = "example_exchange"
symbol = "AAPL"

instrument_type = "equity"
market_category = "equity"

base_asset = "AAPL"
quote_asset = "USD"

status = "active"
```

Let's examine the fields.

| Field | Meaning |
|---|---|
| `exchange` | The previously registered venue. |
| `symbol` | The native instrument symbol. |
| `instrument_type` | The financial product type. |
| `market_category` | Its market category. |
| `base_asset` | The traded asset represented by the instrument. |
| `quote_asset` | The asset or currency used to quote its price. |
| `status` | The registered instrument status. |

For this example, AAPL represents the traded shares and USD is the price currency.

The equity instrument and market category are both `equity`.

For derivatives, the market category may instead describe linear or inverse contract conventions.

The applicable quantity and contract rules are established through specifications, which we will register separately.

### Why are instrument type and market category separate?

Consider a perpetual derivative.

Its instrument type is:

```toml
instrument_type = "perpetual"
```

This tells MarketForge that the instrument is a perpetual contract.

However, perpetual contracts can follow different economic conventions.

For example:

```toml
market_category = "linear"
```

or:

```toml
market_category = "inverse"
```

These categories affect how contract specifications and quantities must be interpreted.

Keeping them separate also helps MarketForge distinguish different products that share similar native symbols.

Do not determine an instrument's market category solely from its symbol.

Use the actual venue and product information.

### Validate the instrument declaration

Run:

```bash
mf catalog instrument validate \
    --file docs/content/examples/catalogue/instrument.toml
```

This checks the local declaration.

Like exchange validation, it does not write anything to PostgreSQL.

A valid declaration is not proof that the exchange or instrument exists in the database.

It only confirms that the declaration follows the expected structure.

### Add the instrument

Run:

```bash
mf catalog instrument add \
    --from docs/content/examples/catalogue/instrument.toml \
    --testing
```

MarketForge checks the declaration and registers the instrument under Example Exchange.

The exchange must already exist.

If the exchange reference cannot be resolved, the instrument cannot be registered successfully.

### How does MarketForge identify the instrument internally?

When an instrument is registered, PostgreSQL assigns its catalogue identity.

MarketForge also establishes the historical instrument mapping used by later processing.

You do not need to manually choose those database IDs.

Instead, you refer to instruments using their catalogue references.

For our example:

```text
example_exchange.equity.equity.AAPL
```

This reference is stable and readable.

It can be used by later declarations and registration recipes.

### List registered instruments

Run:

```bash
mf catalog instrument list --testing
```

This displays instruments stored in the testing catalogue.

### Inspect the AAPL instrument

Run:

```bash
mf catalog instrument show \
    example_exchange.equity.equity.AAPL \
    --testing
```

You can also request JSON output:

```bash
mf --json catalog instrument show \
    example_exchange.equity.equity.AAPL \
    --testing
```

The global `--json` option must appear before `catalog`.

Inspect the returned instrument information and confirm that the exchange, symbol, instrument type, market category, and asset identities match the declaration.

### What happens if an instrument already exists?

If you submit the same instrument declaration again, MarketForge can reuse the existing identity.

However, conflicting properties under the same instrument identity are rejected.

This prevents a registered instrument from silently changing its meaning.

For example, a previously registered instrument should not suddenly acquire a different base asset or contract classification merely because a new declaration was submitted.

MarketForge requires the conflict to be resolved explicitly.

<!-- SCREENSHOT PLACEHOLDER: catalogue-instrument.png

Suggested image:
Terminal output showing:

mf catalog instrument show example_exchange.equity.equity.AAPL --testing

Capture:
- Canonical instrument reference
- Exchange
- Symbol
- Instrument type
- Market category
- Asset information
- Database identity, if displayed

Purpose:
Show what a registered instrument looks like.

Suggested alt text:
"MarketForge showing a registered AAPL equity instrument at Example Exchange."

-->

---

## 6. Register Instrument Specifications

We now know that Example Exchange exists and that AAPL is a registered equity instrument.

However, there is still important information missing.

**MarketForge needs to know how prices and quantities should be interpreted.**

This is the purpose of instrument specifications.

### Why are specifications necessary?

Consider a historical trade:

```text
Price:    233.62
Quantity: 10
```

For our synthetic AAPL recording, we want MarketForge to interpret this as:

```text
Price:    233.62 USD per share
Quantity: 10 shares
```

Now consider a derivative trade:

```text
Price:    80000
Quantity: 100
```

The quantity may represent contracts rather than BTC.

If every contract represents a particular amount of USD or BTC, MarketForge needs the appropriate contract specification to interpret that quantity.

Without this information, treating the raw quantity as a base-asset amount could produce incorrect results.

### What information belongs in specifications?

Depending on the instrument, specifications may describe:

| Property | Purpose |
|---|---|
| Quantity type | Whether quantities represent base units, quote units, or contracts. |
| Tick size | The applicable price increment. |
| Quantity step | The applicable quantity increment. |
| Contract value | The economic value represented by one contract. |
| Contract value asset | The currency or asset in which contract value is expressed. |
| Historical validity | The period during which the specifications apply. |
| Provenance | Where the specification information came from. |

Not every instrument requires every property.

For example, our synthetic equity recording uses share quantities and does not need a derivative contract multiplier.

A contract-denominated derivative requires additional information about its contract value and denomination.

### What is quantity type?

Quantity type tells MarketForge what the raw quantity represents.

The supported categories include:

| Value | Meaning |
|---|---|
| `base` | Quantity is expressed in units of the traded base asset. |
| `quote` | Quantity is expressed in units of the quote asset. |
| `contracts` | Quantity is expressed as a number of derivative contracts. |

For example:

```toml
quantity_type = "base"
```

For our synthetic AAPL recording, this means quantities represent shares.

For a contract-denominated derivative, the declaration must provide the necessary contract value and asset denomination.

### What is tick size?

Tick size describes the applicable price increment.

For example:

```toml
tick_size = "0.01"
```

This declares a price increment of 0.01 in the relevant price denomination.

For our synthetic example, this is one cent.

A real instrument may use a different increment, and historical increments can differ from current ones.

Use the actual rules applicable to the instrument and period.

### What is quantity step?

Quantity step describes the applicable quantity increment.

For example:

```toml
qty_step = "1"
```

For our synthetic share-based example, this declares whole-share increments.

This is an assumption of the example, not a universal rule for all equity trading.

### Why are decimal values written as strings?

Notice that our economic values use quotation marks:

```toml
tick_size = "0.01"
qty_step = "1"
```

MarketForge accepts exact decimal representations for these values.

Using decimal strings avoids introducing unnecessary floating-point approximation when declaring financial specifications.

This is especially important for financial data because small precision errors can affect price and quantity validation.

Use quoted decimals or supported exact TOML integer values for the relevant economic fields.

---

## 7. Historical Specifications: Why Dates Matter

Instrument specifications can change over time.

This is one of the most important ideas in the catalogue.

### The problem with using today's specifications

Imagine downloading BTCUSDT historical data from several years ago.

Today, the exchange reports a particular minimum price increment or contract specification.

However, that value might have been different when the historical trades occurred.

If MarketForge applies today's rules to older recordings, it may interpret or validate the data incorrectly.

The same concern applies to futures contracts, instrument denominations, and other market rules.

**Historical data must be interpreted using specifications that apply to its historical period.**

This is why MarketForge allows specifications to have explicit validity intervals.

### Example: specifications valid for one day

Our synthetic historical declaration contains:

```toml
instrument = "example_exchange.equity.equity.AAPL"

quantity_type = "base"
tick_size = "0.01"
qty_step = "1"

valid_from = 2024-10-28T00:00:00Z
valid_to = 2024-10-29T00:00:00Z

provenance = "Synthetic example parameters for the example files only"
```

Let's examine the important parts.

The instrument field identifies the registered instrument:

```toml
instrument = "example_exchange.equity.equity.AAPL"
```

The economic fields describe how its recorded values should be interpreted:

```toml
quantity_type = "base"
tick_size = "0.01"
qty_step = "1"
```

The validity interval declares when these values apply:

```toml
valid_from = 2024-10-28T00:00:00Z
valid_to = 2024-10-29T00:00:00Z
```

The start is included and the end is excluded.

This declaration therefore covers October 28, 2024, in UTC.

### Why does the interval need to cover the data?

Suppose your recording covers October 28.

MarketForge needs specifications that apply throughout the period represented by the input.

If your specification only covers part of that period, the remaining data may not have a valid economic interpretation.

For example:

```text
Recording:
October 28 ───────────────────── October 29

Specifications:
October 28 ───────── October 28 12:00
```

The declaration covers only half the recording.

That is not sufficient to establish the required rules for the entire input.

For historical archive registration, the applicable specification must cover the complete declared archive interval.

Later normalization preparation also checks the applicability of the specifications to selected processing inputs, including archive portions that may extend outside the requested interval.

### Can an instrument have multiple historical specifications?

Yes.

For example, imagine an exchange changes an instrument's tick size.

The historical specifications might be represented as:

```text
Period A
2024-01-01 ─────────────── 2024-06-01
Tick size: 0.10

Period B
2024-06-01 ─────────────── 2025-01-01
Tick size: 0.01
```

These are separate validity intervals.

MarketForge can retain distinct specifications for adjacent or non-overlapping periods.

However, conflicting specifications for overlapping periods are rejected.

This prevents the catalogue from silently choosing between contradictory historical rules.

### What is provenance?

Provenance explains where the information came from.

For example, a historical specification might come from:

- Official exchange documentation.
- A historical contract specification published by the venue.
- A recorded exchange metadata response.
- A verified dataset specification supplied by a provider.
- A documented manual declaration.

Provenance is important because the correctness of historical interpretation depends on the reliability of the underlying information.

For our synthetic example, the provenance explicitly states:

```toml
provenance = "Synthetic example parameters for the example files only"
```

This is deliberate.

The values exist only to demonstrate how the catalogue works.

They are not verified rules for real historical AAPL trading.

**Do not present values inferred from a few observed records as verified historical exchange specifications.**

For real datasets, use evidence that supports the declared values and their period of applicability.

---

## 8. Add Historical Specifications

Now that we understand the purpose of historical specifications, we can register the example declaration.

### Validate the declaration

Run:

```bash
mf catalog specs validate \
    --file docs/content/examples/catalogue/specs.toml
```

MarketForge checks the TOML declaration.

This does not connect to PostgreSQL or create a specification record.

It verifies the local declaration's structure and values.

### Add the specifications

Run:

```bash
mf catalog specs add \
    --from docs/content/examples/catalogue/specs.toml \
    --testing
```

MarketForge checks that the referenced instrument exists and registers the historical specification evidence.

The instrument must already be registered.

### List specifications

Run:

```bash
mf catalog specs list --testing
```

This lists stored specification information, including current observations and historical evidence.

### Inspect an instrument's specifications

Run:

```bash
mf catalog specs show \
    example_exchange.equity.equity.AAPL \
    --testing
```

For detailed machine-readable information:

```bash
mf --json catalog specs show \
    example_exchange.equity.equity.AAPL \
    --testing
```

Inspect the returned values and historical validity information.

In particular, confirm that the applicable quantity type, tick size, quantity step, and validity interval match the submitted declaration.

### What happens if you register the same specifications again?

Historical specification declarations are retained as immutable evidence.

If you submit the same declaration again, MarketForge can reuse the existing evidence rather than creating a conflicting copy.

If you submit different values for an overlapping period, MarketForge rejects the conflict.

Different values can be registered for separate historical periods where the validity rules allow them.

This helps preserve an auditable history of the information used to interpret market data.

<!-- SCREENSHOT PLACEHOLDER: catalogue-specifications.png

Suggested image:
Terminal output showing:

mf catalog specs show example_exchange.equity.equity.AAPL --testing

Capture:
- Instrument reference
- Quantity type
- Tick size
- Quantity step
- Validity interval
- Provenance
- Evidence identifiers, if displayed

Purpose:
Show how MarketForge records historical economic specifications.

Suggested alt text:
"MarketForge displaying historical instrument specifications and their validity period."

-->

---

## 9. Current Specifications vs. Historical Specifications

MarketForge distinguishes between current observations and historical declarations.

This distinction is important when working with historical market data.

### What is a current observation?

Suppose you query an exchange API today.

The exchange reports the current tick size, quantity step, and other instrument settings.

You can record that information as a current observation.

For example:

```toml
instrument = "example_exchange.equity.equity.AAPL"

quantity_type = "base"
tick_size = "0.01"
qty_step = "1"

fetched_at = 2024-10-28T00:00:00Z

provenance = "Synthetic observation for the example"
```

This example records an observation timestamp:

```toml
fetched_at = 2024-10-28T00:00:00Z
```

It does not declare a historical validity interval.

The timestamp tells us when the information was observed.

It does not establish how long those values were valid before or after that observation.

### Why isn't a current observation enough?

Imagine requesting current metadata for BTCUSDT in 2026.

The exchange returns:

```text
Tick size: 0.10
```

This tells us the tick size reported at the observation time.

It does not prove that the same value applied in 2022.

If MarketForge automatically assumed that current settings were historically valid, it could silently apply incorrect rules to older data.

For this reason:

**A current observation does not establish historical applicability.**

Historical registration requires applicable historical evidence.

A current observation alone cannot satisfy that requirement for an external archive.

### How does MarketForge retain current observations?

A current observation is stored with its observation time and provenance.

When a newer observation is registered, it can advance the catalogue's current information.

An older observation can still be retained as evidence.

This allows MarketForge to keep historical records of what was observed without pretending that an observation proves a complete period of validity.

### Register a current observation

The repository includes an illustrative current-specification declaration.

Validate it:

```bash
mf catalog specs validate \
    --file docs/content/examples/catalogue/current-specs.toml
```

Register it:

```bash
mf catalog specs add \
    --from docs/content/examples/catalogue/current-specs.toml \
    --testing
```

Then inspect the stored specification information:

```bash
mf catalog specs show \
    example_exchange.equity.equity.AAPL \
    --testing
```

Remember that the example values are synthetic.

### Comparing the two types of evidence

| Property | Current observation | Historical declaration |
|---|---|---|
| Records economic values | Yes | Yes |
| Records provenance | Yes | Yes |
| Records observation time | Yes, when supplied | May also include an observation time |
| Declares a validity interval | No | Yes |
| Proves applicability to older recordings | No | Only for the supported declared interval and evidence |
| Can be retained as evidence | Yes | Yes |

When choosing specifications for historical normalization, the key question is not merely which values are newest.

It is which values are supported for the period being processed.

---

## 10. Register the Complete Example

So far, we have examined the three catalogue components separately.

Now we can register the complete synthetic example in the correct order.

The repository provides these files:

```text
docs/content/examples/catalogue/
├── exchange.toml
├── instrument.toml
├── specs.toml
└── current-specs.toml
```

For the main workflow, we will use:

```text
exchange.toml
instrument.toml
specs.toml
```

The current-specifications file is optional and demonstrates a different type of evidence.

### Step 1: Register the exchange

```bash
mf catalog exchange validate \
    --file docs/content/examples/catalogue/exchange.toml

mf catalog exchange add \
    --from docs/content/examples/catalogue/exchange.toml \
    --testing
```

This establishes the fictional venue.

### Step 2: Register the instrument

```bash
mf catalog instrument validate \
    --file docs/content/examples/catalogue/instrument.toml

mf catalog instrument add \
    --from docs/content/examples/catalogue/instrument.toml \
    --testing
```

This establishes AAPL as an equity instrument belonging to Example Exchange.

### Step 3: Register the specifications

```bash
mf catalog specs validate \
    --file docs/content/examples/catalogue/specs.toml

mf catalog specs add \
    --from docs/content/examples/catalogue/specs.toml \
    --testing
```

This adds the synthetic economic specifications for October 28, 2024.

### Step 4: Inspect the registered records

Check the exchange:

```bash
mf catalog exchange show example_exchange --testing
```

Check the instrument:

```bash
mf catalog instrument show \
    example_exchange.equity.equity.AAPL \
    --testing
```

Check the specifications:

```bash
mf catalog specs show \
    example_exchange.equity.equity.AAPL \
    --testing
```

At this point, the testing catalogue should contain the three registered components.

### What have we accomplished?

We have established:

```text
Exchange
    example_exchange
         |
         v
Instrument
    example_exchange.equity.equity.AAPL
         |
         v
Historical Specifications
    October 28, 2024
    Quantity type: base
    Tick size: 0.01
    Quantity step: 1
```

MarketForge now has the catalogue information required by the corresponding synthetic raw-data registration example.

We have not downloaded, imported, or normalized any market-data files yet.

Those are separate operations.

<!-- SCREENSHOT PLACEHOLDER: catalogue-complete.png

Suggested image:
The final registered exchange, instrument, and
historical specification inspection results.

Capture:
- All three identities
- Successful registration
- Historical specification validity

Purpose:
Show the expected outcome of the complete catalogue workflow.

Suggested alt text:
"Complete MarketForge catalogue registration for the synthetic AAPL instrument."

-->

---

## 11. How Catalogue Information Is Used Later

Registering a catalogue entry is only one part of the historical data workflow.

The catalogue becomes particularly useful when MarketForge needs to associate raw records with the correct instrument.

For example, consider the synthetic trade file:

```text
1730125800000000000,233.62,10,B
```

This raw record contains:

- A timestamp.
- A price.
- A quantity.
- A trade-side code.

However, it does not identify the exchange or fully explain the economic meaning of its fields.

MarketForge needs additional information.

### The instrument identifies the market

The registered instrument tells MarketForge that the recording belongs to:

```text
example_exchange.equity.equity.AAPL
```

### The specifications explain the quantities

The specifications tell MarketForge that the quantity uses base units.

For this synthetic equity recording, the quantity represents shares.

### The profile explains the file format

A separate normalization profile tells MarketForge how to read the source record.

For example, it identifies which CSV column contains the timestamp, price, quantity, and trade side.

It also describes how to interpret those values.

### Raw-data registration connects everything

The registration recipe associates the source file with:

- Its provider and origin.
- The registered instrument.
- Its applicable historical specifications.
- The normalization profile.
- Its declared historical coverage.

The resulting workflow is:

```text
                Raw CSV / JSONL
                      |
                      v
              Source Registration
                      |
          +-----------+-----------+
          |                       |
          v                       v
      Catalogue                 Profile
          |                       |
          v                       v
   Instrument Identity      Field Mapping
          |
          v
   Historical Specifications
          |
          +-----------+-----------+
                      |
                      v
                 Normalize
                      |
                      v
              Canonical Parquet
```

This separation is intentional.

It means a profile can describe a source format without having to repeat all the economic information about every instrument.

It also means the same registered instrument can be associated with multiple files from different sources when the required identities and interpretation rules are established.

---

## 12. Catalogue vs. Profiles vs. Raw Registration

These three concepts are closely related, but they serve different purposes.

Understanding the distinction makes external data integration much easier.

### Catalogue

The catalogue answers:

**"What financial instrument does this data represent?"**

It describes the venue, instrument identity, assets, and applicable economic specifications.

### Profile

A normalization profile answers:

**"How do we read and interpret the fields in this file?"**

It describes source formats, timestamps, prices, quantities, sides, and other supported transformations.

For example, one provider might store timestamps in nanoseconds while another uses ISO 8601 strings.

The profile explains those differences.

### Raw registration

Raw registration answers:

**"Where did these files come from, and which catalogue instrument and profile should be used?"**

It connects physical source files to their registered market identities and interpretation rules.

### Practical comparison

| Component | Example information |
|---|---|
| Catalogue | AAPL equity at Example Exchange, quoted in USD. |
| Specifications | Quantities are shares; applicable price and quantity increments. |
| Profile | CSV column 1 is timestamp, column 2 is price, column 3 is quantity. |
| Registration | `synthetic-trades.csv` belongs to the registered AAPL instrument and uses the selected trade profile. |

The catalogue does not replace profiles.

Profiles do not replace historical economic specifications.

Registration connects the existing declarations to concrete raw files.

### What comes next?

Once the catalogue is ready, continue with the corresponding profile and registration workflows:

- [Create normalization profiles](../../workflows/catalogue/profiles.md)
- [Register external historical data](../../workflows/acquisition/external-data.md)
- [Synthetic trades and depth example](../../examples/catalogue/index.md)

---

## 13. Providers and Exchanges Are Not Always the Same

When working with external market data, it is especially important to distinguish the company that supplies the data from the market where trading actually happened.

Consider Databento.

Databento distributes historical market data from different venues.

Suppose you purchase an AAPL recording from the `XNAS.ITCH` dataset.

In this case:

| Identity | Value |
|---|---|
| Data provider | Databento |
| Dataset or product | XNAS.ITCH |
| Trading venue | Nasdaq (`xnas`) |
| Instrument | AAPL |
| Instrument type | Equity |

The provider is Databento.

The execution venue is Nasdaq.

**Registering Databento as the exchange would incorrectly describe where the trades occurred.**

MarketForge therefore separates the instrument's venue from the origin of its source files.

### Where does the provider information go?

Provider details belong to the raw-data registration and provenance information.

The catalogue identifies the execution venue and instrument.

This makes it possible to associate data from different providers with the correct market identity.

### What about native instrument IDs?

Some data providers assign their own numeric instrument identifiers.

For example, a recording may contain a provider-specific instrument ID in addition to the familiar symbol.

These identifiers belong to the provider's or publisher's identity system.

They should not be confused with MarketForge's PostgreSQL instrument IDs.

MarketForge preserves source-native identity information in the appropriate source registration metadata.

It uses its own catalogue identities to associate records with the correct instruments.

### When is this important?

This distinction matters when:

- Combining datasets from different providers.
- Registering equities from institutional market-data vendors.
- Working with historical futures or options.
- Processing recordings containing numeric native instrument IDs.
- Investigating inconsistent symbols across sources.

For a complete example, see [Databento AAPL historical data](../../examples/bring-your-own-data/databento/index.md).

---

## 14. Catalogue Command Reference

MarketForge provides the same four basic operations for exchanges, instruments, and specifications.

These commands use the `mf catalog` namespace.

### Supported entities

The current manual catalogue CLI supports:

| Entity | What it manages |
|---|---|
| `exchange` | Execution venues. |
| `instrument` | Individual financial instruments. |
| `specs` | Current and historical instrument specifications. |

The general command structure is:

```text
mf catalog ENTITY ACTION
```

Replace `ENTITY` with one of the supported entity names.

### Available actions

| Action | Purpose |
|---|---|
| `validate` | Check a local declaration without writing to the database. |
| `add` | Validate and register a declaration in the testing catalogue. |
| `list` | List registered catalogue records. |
| `show` | Inspect a specific registered entry. |

### Validate

```text
mf catalog ENTITY validate --file PATH
```

For example:

```bash
mf catalog instrument validate \
    --file docs/content/examples/catalogue/instrument.toml
```

Validation reads the local TOML file.

It does not establish that the corresponding records are already present in PostgreSQL.

### Add

```text
mf catalog ENTITY add --from PATH --testing
```

For example:

```bash
mf catalog instrument add \
    --from docs/content/examples/catalogue/instrument.toml \
    --testing
```

Adding a declaration performs database work.

The command checks the relevant existing identities and rejects conflicting information.

### List

```text
mf catalog ENTITY list --testing
```

For example:

```bash
mf catalog instrument list --testing
```

This lists the corresponding registered records.

### Show

```text
mf catalog ENTITY show REFERENCE --testing
```

For example:

```bash
mf catalog instrument show \
    example_exchange.equity.equity.AAPL \
    --testing
```

The reference format depends on the entity.

| Entity | Reference |
|---|---|
| Exchange | Registered venue code |
| Instrument | `exchange.instrument_type.market_category.symbol` |
| Specifications | The corresponding instrument reference |

### JSON output

All catalogue actions support the global `--json` option.

For example:

```bash
mf --json catalog instrument list --testing
```

```bash
mf --json catalog specs show \
    example_exchange.equity.equity.AAPL \
    --testing
```

The `--json` option must appear before `catalog`.

JSON output is useful when scripting catalogue inspection or examining detailed registration evidence.

### Database behavior

| Command | Requires PostgreSQL | Can write catalogue records |
|---|---|---|
| `validate` | No | No |
| `add` | Yes | Yes |
| `list` | Yes | No |
| `show` | Yes | No |

These commands do not contact exchanges to download historical archives.

Manual catalogue registration also does not require an acquisition adapter, market-statistics refresh, or ranking snapshot.

---

## 15. Repeated Registration and Conflicts

MarketForge is designed to preserve stable instrument identities.

This means repeated submissions should not silently change an existing market definition.

### Registering the same exchange twice

If an exchange declaration matches the existing registered information, MarketForge can reuse it.

A different exchange name under the same code is rejected.

### Registering the same instrument twice

If an instrument declaration matches the existing registered identity and properties, MarketForge can reuse the existing record.

Conflicting instrument properties are rejected.

### Registering the same historical specifications twice

Identical historical evidence can be reused.

Conflicting overlapping validity intervals are rejected.

An adjacent or disjoint historical interval may contain different specifications.

### Why preserve these rules?

Consider historical research performed using a particular contract specification.

If the catalogue silently replaced that specification with newer values, previously processed datasets could become difficult to explain or reproduce.

MarketForge therefore retains historical specification evidence and rejects conflicting declarations instead of silently choosing new values.

The goal is to make historical interpretation traceable.

### Can I change an existing instrument's meaning?

Do not attempt to redefine an existing instrument by submitting incompatible properties under the same identity.

If the original declaration was incorrect, investigate the existing catalogue record and the appropriate supported correction path.

A conflicting `add` operation is not a general-purpose migration or overwrite mechanism.

Similarly, a new historical specification should describe a genuinely applicable period rather than overwrite earlier evidence without justification.

---

## 16. Common Questions

### Does MarketForge automatically register exchanges?

MarketForge can collect market metadata from supported exchange APIs through its market-management workflows.

Manual catalogue registration is available for instruments and venues that need explicit declarations.

You do not need a dedicated API adapter to register a venue manually.

### Do I need to register Bybit manually?

Not necessarily.

If the required Bybit instrument and applicable specifications already exist in the catalogue, you can reuse them.

Manual registration is useful when the necessary information is absent or when you are working with external sources.

### Why can't MarketForge identify everything from the symbol?

Because symbols are not globally unique and may be reused across different markets.

For example, BTCUSDT may identify a spot instrument or a perpetual contract.

MarketForge uses the exchange, instrument type, market category, and symbol together.

### Does registering an exchange make its historical archives available?

No.

Catalogue registration only establishes the venue identity.

It does not implement an acquisition adapter or create an external archive collection.

Downloading historical data from a venue requires a supported acquisition source.

You can still register your own files using the external-data workflow.

### Does adding an instrument download its trading history?

No.

Instrument registration stores metadata about the instrument.

Historical acquisition and raw-data registration are separate operations.

### Why does MarketForge need tick size and quantity type?

Because raw prices and quantities can have different economic meanings depending on the instrument.

For example, a derivative quantity may represent contracts rather than units of the base asset.

MarketForge needs the appropriate specifications to interpret the data correctly.

### Can I use current exchange specifications for old historical data?

Only if their historical applicability has been established through an appropriate evidence mechanism.

A current API response alone does not prove that the same rules applied in the past.

### What if the specifications changed during my historical period?

The applicable historical specifications need to represent the relevant validity periods.

For example, a tick-size change may require separate declarations for the periods before and after the change.

Conflicting overlapping historical rules are rejected.

### Can I register an instrument that no longer trades?

Manual registration is not limited to instruments currently listed by an exchange API.

Historical instruments still require valid identity information and specifications appropriate to their historical data.

Do not fabricate missing contract information merely to make registration succeed.

### Do profiles belong in the catalogue?

Profiles are managed separately.

The catalogue describes the market and instrument.

A profile describes how MarketForge reads a particular source format.

Profiles are validated and installed through `mf acquisition profiles`.

### Can two data providers refer to the same instrument?

Yes, provided the relevant venue and instrument identities are correctly established.

Provider information belongs to the source's provenance and native identity mapping.

The catalogue identifies the market where the instrument belongs.

### Does MarketForge assign numeric instrument IDs?

Yes.

PostgreSQL manages the internal catalogue identities.

Users normally refer to instruments using readable references such as:

```text
bybit.perpetual.linear.BTCUSDT
```

Do not invent database IDs manually.

### Does `validate` register anything?

No.

Validation checks the local TOML declaration.

Use `add --testing` to register the declaration.

### Can I submit the same declaration again?

Yes, when the existing record is compatible.

Exact repeated declarations can be reused, while conflicting definitions are rejected.

### Can I register historical specifications without an instrument?

No.

The historical specification must refer to an existing registered instrument.

Register the exchange first, then the instrument, then its specifications.

### Does `--testing` mean no database changes?

No.

`--testing` selects the guarded testing database.

Commands such as `catalog exchange add` can write real records to that database.

---

## 17. Troubleshooting Catalogue Registration

Most catalogue errors involve invalid declarations, missing dependencies, or conflicting metadata.

### Exchange not found

**Possible cause:** You are trying to register an instrument before registering its exchange.

First inspect the exchange:

```bash
mf catalog exchange show example_exchange --testing
```

If it does not exist, register the appropriate exchange declaration.

Then retry the instrument registration.

### Instrument not found

**Possible cause:** You are registering specifications for an instrument that has not yet been added.

Inspect the instrument:

```bash
mf catalog instrument show \
    example_exchange.equity.equity.AAPL \
    --testing
```

If it is absent, register the instrument declaration first.

### Invalid TOML declaration

**Possible cause:** The file is malformed, missing required fields, or contains unsupported values.

Run the relevant validation command:

```bash
mf catalog specs validate \
    --file docs/content/examples/catalogue/specs.toml
```

Review the reported validation error and correct the declaration.

Validation is useful because it does not modify the database.

### Conflicting exchange or instrument definition

**Possible cause:** A catalogue identity already exists with different registered properties.

Inspect the existing record before retrying.

For example:

```bash
mf catalog instrument show \
    example_exchange.equity.equity.AAPL \
    --testing
```

Compare it with the submitted TOML file.

Do not change the symbol or market classification arbitrarily to bypass the conflict.

### Conflicting historical specifications

**Possible cause:** A new declaration overlaps an existing historical interval but contains incompatible values.

Inspect the existing specification evidence:

```bash
mf --json catalog specs show \
    example_exchange.equity.equity.AAPL \
    --testing
```

Review the validity intervals, economic values, and provenance.

If a genuine specification change occurred, declare the correct applicable periods.

Do not use a new observation timestamp as a substitute for historical validity.

### Missing historical specification coverage

**Possible cause:** The registered specifications do not cover the entire period represented by a raw input.

Inspect the relevant historical specification intervals.

Compare them with the declared source coverage.

Register additional applicable evidence where necessary.

Do not extend a validity interval without evidence that the values were actually valid during the additional period.

### Database connection rejected

**Possible cause:** The testing database is not configured, initialized, or recognized as the expected testing environment.

Check:

```bash
mf database status --testing
```

See [Database setup](../../getting-started/installation.md) for the required credentials and identity checks.

### Profile installation fails after catalogue registration

Catalogue registration and profile installation are separate operations.

If a profile fails validation, inspect its format mappings and the Rust compiler diagnostics.

If installation fails because of a missing venue or an existing conflicting profile identity, inspect the corresponding catalogue and profile records.

See [Profile authoring and troubleshooting](../../workflows/catalogue/profiles.md).

---

## 18. What Comes After Catalogue Registration?

Once the exchange, instrument, and applicable specifications have been registered, MarketForge has the market information needed by the corresponding raw-data workflow.

For external data, the next step is normally to define how the source files should be interpreted and associate those files with their catalogue identities.

For example:

```text
           Registered Exchange
                   |
                   v
           Registered Instrument
                   |
                   v
        Historical Specifications
                   |
                   v
          Normalization Profiles
                   |
                   v
          Raw Data Registration
                   |
                   v
              Normalize
                   |
                   v
          Canonical Parquet
```

You can continue with:

| Next task | Where to go |
|---|---|
| Create a trades or depth profile | [Profile authoring](../../workflows/catalogue/profiles.md) |
| Register existing recordings | [External data registration](../../workflows/acquisition/external-data.md) |
| Follow the complete synthetic example | [Synthetic trade and depth recordings](../../examples/catalogue/index.md) |
| Register a Databento dataset | [Databento AAPL example](../../examples/bring-your-own-data/databento/index.md) |
| Normalize registered raw files | [Historical normalization](../../workflows/normalize.md) |
| Inspect normalized datasets | [Dataset inspection](../../workflows/datasets.md) |

### The important distinction

**Catalogue** identifies the financial instruments and their economic rules.

**Profiles** explain how to read the source file formats.

**Raw registration** connects files to the correct instruments and profiles.

**Normalization** converts those files into a consistent market-data representation.

Together, these components allow MarketForge to process historical data from supported exchanges and user-supplied sources without losing the information needed to interpret it correctly.
