# alp-data-engineer
final project data engineer
Extract, Transform and Load data silver for detection scam transaction

#Tech Stack use
1. python
2. postgresql

## 1) Setup
```bash
pip install -r requirements.txt
cp .env.example .env  # if you have an example; otherwise create .env
```

Minimal .env:
```env
DB_HOST=...
DB_PORT=...
DB_USER=...
DB_PASSWORD=...
DB_NAME=...
# or DATABASE_URL=postgresql://user:password@host:port/dbname?sslmode=require
```

Verify setup:
```bash
python run_pipeline_complete.py --dry-run
```

## 2) Run

# Quick usage (TL;DR)

End-to-end (near-realtime):
```bash
python run_pipeline_complete.py --mode complete --watch --interval 5
```

Manual steps:
```bash
# Generate (realtime CSV + DB)
python generateTransactionsBronze.py --rate 100 --fraud-ratio 0.4 --csv-batch-size 1
# Ingest CSV to bronze (only if you disabled realtime DB)
python -c "from generateTransactionsBronze import ingest_daily_csv_to_db; ingest_daily_csv_to_db()"
# Bronze → Silver
python run_pipeline_complete.py --mode silver-only
# Silver → Gold
python run_pipeline_complete.py --mode gold-only
```

Stop any command with Ctrl+C.

## How to run the transaction generator (bronze)

This generates synthetic transfer transactions into PostgreSQL table `bronze.transactions_raw` and reads BNI beneficiaries and sender accounts from `bronze.data_nasabah_raw`.

Prerequisites:
- Python 3.9+
- PostgreSQL database reachable from your machine
- Tables exist: `bronze.data_nasabah_raw` (with `name`, `account_number`, `status='OPENED'`) and `bronze.transactions_raw`

Install deps (already done in Setup) if needed:
```bash
pip install -r requirements.txt
```

Configure database connection:
- Edit the `DB_CONNECTION` constant at the top of `alp-data-engineer/generateTransactionsBronze.py` to your PostgreSQL URI, for example:
  `postgresql://user:password@host:port/dbname?sslmode=require`

Quick commands:
```bash
# Default (60 tx/min), fraud 0.25, repeat 0.15, unlimited until Ctrl+C, realtime CSV and DB insert
python generateTransactionsBronze.py

# Custom rate (120 tx/min) and cap at 1,000 transactions
python generateTransactionsBronze.py --rate 120 --max-transactions 1000

# Tweak fraud behavior
python generateTransactionsBronze.py --fraud-ratio 0.3 --repeat-prob 0.2

# CSV batching: flush to CSV every 100 rows (still realtime DB insert by default)
python generateTransactionsBronze.py --csv-batch-size 100

# CSV only (no realtime DB insert). The pipeline will ingest the CSV later
python generateTransactionsBronze.py --csv-batch-size 100 --no-realtime-db
```

Notes:
- Daily CSV is written to `generated/transactions_raw_YYYYMMDD.csv` (append if same date).
- Realtime DB insert uses `ON CONFLICT(id) DO NOTHING` to skip duplicates.
- Stop with Ctrl+C; progress and basic fraud stats print every 100 rows.
- If `bronze.data_nasabah_raw` is empty or unreachable, the script falls back to random sender accounts; BNI beneficiaries load may be empty.

### Ingest today's CSV into bronze manually
```bash
python -c "from generateTransactionsBronze import ingest_daily_csv_to_db; ingest_daily_csv_to_db()"
```
This loads `generated/transactions_raw_YYYYMMDD.csv` into `bronze.transactions_raw`, inserting only IDs that are not present.

## 3) Complete Pipeline: Bronze → Silver → Gold

### Quick Start

```bash
# Run complete pipeline (Bronze → Silver → Gold)
python run_pipeline_complete.py --mode complete

# Watch mode (near-realtime): re-run the pipeline every 5 seconds
python run_pipeline_complete.py --mode complete --watch --interval 5

# Show status / config
python run_pipeline_complete.py --status
python run_pipeline_complete.py --config
```

### Simplest usage

End-to-end (realtime-ish):
```bash
python run_pipeline_complete.py --mode complete --watch --interval 5
```

Steps one-by-one:
```bash
# 1) Generate transactions (realtime CSV + DB)
python generateTransactionsBronze.py --rate 100 --fraud-ratio 0.4 --csv-batch-size 1

# 2) (If CSV-only was used) Ingest today's CSV into bronze
python -c "from generateTransactionsBronze import ingest_daily_csv_to_db; ingest_daily_csv_to_db()"

# 3) Bronze → Silver only
python run_pipeline_complete.py --mode silver-only

# 4) Silver → Gold only
python run_pipeline_complete.py --mode gold-only
```
Stop any command with Ctrl+C.

### Modes

```bash
# Full pipeline
python run_pipeline_complete.py --mode complete

# Bronze only
python run_pipeline_complete.py --mode bronze-only

# Gold only (requires silver data)
python run_pipeline_complete.py --mode gold-only

# Dry run (validate DB/config)
python run_pipeline_complete.py --dry-run
```

#### Pipeline runner parameters

```bash
python run_pipeline_complete.py [--mode {complete,bronze-only,silver-only,gold-only}] \
  [--dry-run] [--status] [--config] [--verbose|-v] \
  [--watch] [--interval SECONDS]

# Options:
# --mode       : Execution mode (default: complete)
# --dry-run    : Validate DB and config without executing
# --status     : Show current pipeline status/metrics and exit
# --config     : Show current pipeline configuration and exit
# --verbose|-v : Enable verbose logging (DEBUG)
# --watch      : Continuously re-run the pipeline
# --interval   : Seconds between runs in watch mode (default: 60)
```

#### Generator parameters (standalone)

```bash
python generateTransactionsBronze.py \
  [--rate INT] [--fraud-ratio FLOAT] [--repeat-prob FLOAT] \
  [--max-transactions INT] [--csv-batch-size INT] [--no-realtime-db]

# Options:
# --rate             : Transactions per minute (default: 60)
# --fraud-ratio      : Probability a tx participates in fraud pattern (default: 0.25)
# --repeat-prob      : Probability to reuse a prior beneficiary (default: 0.15)
# --max-transactions : Stop after N transactions (omit or 0 = unlimited)
# --csv-batch-size   : Flush to CSV every N rows (1 = realtime)
# --no-realtime-db   : Disable realtime insert into bronze.transactions_raw
```

#### Generator parameters (inside pipeline)
Configured in `pipeline_config.py` (defaults) or override with `pipeline_config.json`:

```json
{
  "generator": {
    "enabled": true,
    "rate_per_minute": 100,
    "max_transactions": 0,
    "fraud_ratio": 0.4,
    "repeat_probability": 0.15,
    "csv_batch_size": 1,
    "realtime_db": true
  }
}
```

Notes: `max_transactions: 0` means unlimited. When enabled, the pipeline will generate, ingest today's CSV, then process silver and gold.

### Configuration
The pipeline reads defaults from `pipeline_config.py` and overrides from `pipeline_config.json` (if present). Database settings come from `.env`.

## 4) Deduplication rules
- Bronze ingestion: skips existing IDs using pre-check and `ON CONFLICT(id) DO NOTHING`.
- Silver insert: skips already inserted transformed rows (existing project logic).
- Gold insert:
  - `transactions_normal` and `transactions_abnormal`: skip if `id` exists.
  - `transactions_summary`: skip if `(trx_date, tipe_anomali)` exists.

 
