# alp-data-engineer
final project data engineer
Extract, Transform and Load data silver for detection scam transaction

#Tech Stack use
1. python
2. postgresql

# configuration with .env
1. create file .env
2. add the configuration variables below and adjust them to those used locally
DB_HOST={HOST}
DB_PORT={PORT}
DB_USER={USER}
DB_PASSWORD={PASSWORD}
DB_NAME={DBNAME}
DB_SSLMODE={require|prefer|disable}  # optional

# Or provide a full URL instead of the above:
DATABASE_URL=postgresql://user:password@host:port/dbname?sslmode=require

# Optional: where silver CSV outputs are written (defaults to ./generated)
PATH_FILE_GENERATED=/absolute/path/to/output

## How to run the transaction generator (bronze)

This generates synthetic transfer transactions into PostgreSQL table `bronze.transactions_raw` and reads BNI beneficiaries and sender accounts from `bronze.data_nasabah_raw`.

Prerequisites:
- Python 3.9+
- PostgreSQL database reachable from your machine
- Tables exist: `bronze.data_nasabah_raw` (with `name`, `account_number`, `status='OPENED'`) and `bronze.transactions_raw`

Install dependencies (no virtualenv):
```bash
pip install --upgrade pip
pip install psycopg2-binary
```

Conda setup (alternative to venv):
```bash
# Create and activate a new conda environment
conda create -n alp-de python=3.10 -y
conda activate alp-de

# Install dependencies
pip install psycopg2-binary
```

Configure database connection:
- Edit the `DB_CONNECTION` constant at the top of `alp-data-engineer/generateTransactionsBronze.py` to your PostgreSQL URI, for example:
  `postgresql://user:password@host:port/dbname?sslmode=require`

Run examples:
```bash
# Default rate (60 tx/min), fraud ratio 0.25, repeat prob 0.15, unlimited until Ctrl+C
python generateTransactionsBronze.py

# Custom rate (120 tx/min) and cap at 1,000 transactions
python generateTransactionsBronze.py --rate 120 --max-transactions 1000

# Tweak fraud behavior
python generateTransactionsBronze.py --fraud-ratio 0.3 --repeat-prob 0.2
```

Notes:
- Stop with Ctrl+C; progress and basic fraud stats print every 100 rows.
- If `bronze.data_nasabah_raw` is empty or unreachable, the script falls back to random sender accounts; BNI beneficiaries load may be empty.

## Run full pipeline (bronze -> silver) in parallel

Use the helper script `run_pipeline.sh` to start the bronze generator and execute the silver transformation periodically in parallel.

```bash
# Make sure it's executable (already set in repo, but just in case)
chmod +x run_pipeline.sh

# Run with defaults (120 tx/min, fraud 0.25, repeat 0.15, silver every 30s)
./run_pipeline.sh

# Override parameters via environment variables
RATE_PER_MINUTE=200 FRAUD_RATIO=0.3 REPEAT_PROB=0.2 SILVER_INTERVAL_SEC=20 \
  ./run_pipeline.sh

# Limit total generated transactions (pipeline exits after bronze completes)
MAX_TRANSACTIONS=1000 ./run_pipeline.sh
```

Behavior:
- Bronze generator runs in background, streaming into `bronze.transactions_raw`.
- Silver job (`alp-data-engineer/main.py`) runs every `SILVER_INTERVAL_SEC` and writes to `silver.transactions` and CSV output per your `.env` (`PATH_FILE_GENERATED`).
- Script traps Ctrl+C to stop bronze and exit cleanly.

### Start/Stop helpers (background)

```bash
# Start in background (creates .pipeline.pid; logs cleaned on stop)
./start_pipeline.sh

# Stop background pipeline (also removes *.log and *.out)
./stop_pipeline.sh
```

## Complete Pipeline: Bronze → Silver → Gold

### Quick Start

```bash
# Run complete pipeline (Bronze → Silver → Gold)
python run_pipeline_complete.py --mode complete

# Or use the enhanced shell runner (validates, runs periodically)
./run_pipeline.sh
```

### Pipeline Manager

```bash
# Start / Stop / Restart
./pipeline_manager.sh start
./pipeline_manager.sh stop
./pipeline_manager.sh restart

# Status, Logs, Config, Validate
./pipeline_manager.sh status
./pipeline_manager.sh logs
./pipeline_manager.sh config
./pipeline_manager.sh validate
```

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

### Monitoring & Metrics

```bash
# Show live status and recent history
python run_pipeline_complete.py --status

# Logs are written during runs and cleaned on stop
```

### Configuration

The pipeline uses `pipeline_config.py` for settings (timeouts, batch sizes, layer enable/disable). Environment variables are loaded from `.env`.

## Environment setup (Conda) and dependencies

```bash
# 1) Create and activate a conda env
conda create -n alp-de python=3.10 -y
conda activate alp-de

# 2) Install project dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 3) Configure environment variables
cp .env.example .env  # if you keep an example file; otherwise create .env
# then edit .env to match your database settings

# 4) (Optional) Verify pipeline is ready
python run_pipeline_complete.py --dry-run
```
