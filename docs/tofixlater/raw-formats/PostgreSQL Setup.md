## Quick Navigation

- [Purpose](#purpose)
- [Install PostgreSQL](#install-postgresql)
- [Verify PostgreSQL](#verify-postgresql)
- [Create the MarketForge Role](#create-the-marketforge-role)
- [Create the MarketForge Database](#create-the-marketforge-database)
- [Test the Connection](#test-the-connection)
- [Create PostgreSQL Schemas](#create-postgresql-schemas)
- [Configure MarketForge](#configure-marketforge)
- [Useful Commands](#useful-commands)
- [Storage Layout](#storage-layout)

## Purpose

MarketForge uses PostgreSQL for:

```text
persistent instrument metadata
normalization metadata
recent trades
recent L2 snapshots
recent L2 updates
bootstrap / recovery data
```

Long-term normalized market data remains in Parquet.

The intended storage model is:

```text
PostgreSQL
├── catalog
│   ├── exchanges
│   ├── instruments
│   ├── instrument_specs
│   ├── instrument_api_raw
│   ├── raw_formats
│   └── normalization_rules
│
└── live
    ├── trades
    ├── l2_snapshots
    └── l2_updates

Parquet
└── permanent historical market data
```

PostgreSQL is not used as the latency-critical transport between MarketForge and trading software.

Live market data is distributed separately through local IPC.

---

## Install PostgreSQL

These instructions assume Ubuntu/Debian Linux.

Update the package index:

```bash
sudo apt update
```

Install PostgreSQL:

```bash
sudo apt install postgresql postgresql-contrib
```

Enable and start PostgreSQL:

```bash
sudo systemctl enable --now postgresql
```

Check its status:

```bash
systemctl status postgresql
```

Check the installed client version:

```bash
psql --version
```

---

## Verify PostgreSQL

Ubuntu creates a local system account named:

```text
postgres
```

Open the PostgreSQL shell as the administrative PostgreSQL user:

```bash
sudo -u postgres psql
```

Verify the server:

```sql
SELECT version();
```

Exit:

```sql
\q
```

---

## Create the MarketForge Role

Open the PostgreSQL shell:

```bash
sudo -u postgres psql
```

Create a dedicated application role:

```sql
CREATE ROLE marketforge
WITH LOGIN PASSWORD 'YOUR_PASSWORD';
```

Use a strong local password and do not commit it to Git.

The MarketForge application should use this role rather than the PostgreSQL administrative account.

---

## Create the MarketForge Database

While still connected as the PostgreSQL administrator:

```sql
CREATE DATABASE marketforge
OWNER marketforge;
```

The resulting structure is:

```text
PostgreSQL
└── marketforge database
    └── owned by marketforge role
```

Exit:

```sql
\q
```

---

## Test the Connection

Connect through localhost using the MarketForge role:

```bash
psql \
  -h 127.0.0.1 \
  -U marketforge \
  -d marketforge
```

Enter the password created earlier.

Verify the connection:

```sql
SELECT current_database(), current_user;
```

Expected:

```text
current_database | current_user
-----------------+-------------
marketforge      | marketforge
```

Exit:

```sql
\q
```

Using `127.0.0.1` explicitly tests the TCP connection that MarketForge can later use from Python and Rust.

---

## Create PostgreSQL Schemas

Connect:

```bash
psql -h 127.0.0.1 -U marketforge -d marketforge
```

Create the two MarketForge namespaces:

```sql
CREATE SCHEMA catalog;
CREATE SCHEMA live;
```

Verify them:

```sql
\dn
```

The database should now contain:

```text
catalog
live
public
```

Their responsibilities are:

```text
catalog
    persistent metadata and normalization knowledge

live
    rolling recent market data used for bootstrap and recovery
```

Do not manually create the MarketForge tables.

Table creation and migrations should be owned by MarketForge's database schema code.

---

## Configure MarketForge

Create a local environment file in the repository root:

```bash
touch .env
```

Add:

```text
MARKETFORGE_DATABASE_URL=postgresql://marketforge:YOUR_PASSWORD@127.0.0.1:5432/marketforge
```

The real `.env` file must remain ignored by Git.

The repository should contain:

```text
.env.example
```

with a non-secret example:

```text
MARKETFORGE_DATABASE_URL=postgresql://marketforge:password@127.0.0.1:5432/marketforge
```

Do not store real database passwords in:

```text
source code
configuration committed to Git
documentation
notebooks
tests
```

---

## Useful Commands

Start PostgreSQL:

```bash
sudo systemctl start postgresql
```

Stop PostgreSQL:

```bash
sudo systemctl stop postgresql
```

Restart PostgreSQL:

```bash
sudo systemctl restart postgresql
```

Check status:

```bash
systemctl status postgresql
```

Connect as MarketForge:

```bash
psql -h 127.0.0.1 -U marketforge -d marketforge
```

Connect as PostgreSQL administrator:

```bash
sudo -u postgres psql
```

List databases:

```sql
\l
```

List roles:

```sql
\du
```

List schemas:

```sql
\dn
```

List tables:

```sql
\dt catalog.*
\dt live.*
```

Describe a table:

```sql
\d catalog.instruments
```

Exit:

```sql
\q
```

---

## Storage Layout

The persistent catalog will contain:

```text
catalog.exchanges
catalog.instruments
catalog.instrument_specs
catalog.instrument_api_raw
catalog.raw_formats
catalog.normalization_rules
```

The rolling live store will eventually contain:

```text
live.trades
live.l2_snapshots
live.l2_updates
```

The initial target retention is approximately:

```text
trades      ≈ 24 hours
L2 updates  ≈ 24 hours
L2 snapshots > 24 hours
```

Complete L2 snapshots will be persisted periodically, initially around:

```text
30 minutes
```

Bootstrap then becomes:

```text
latest complete L2 snapshot
        ↓
replay subsequent L2 updates
        ↓
current reconstructed book
        ↓
synchronize with live stream
```

Permanent historical data remains outside PostgreSQL:

```text
PostgreSQL
    metadata + rolling bootstrap data

Parquet
    permanent normalized history

Rust IPC
    low-latency live market data
```

PostgreSQL should initially use its standard configuration. Database tuning, advanced partitioning, and TimescaleDB should only be introduced after the real live workload has been implemented and benchmarked.