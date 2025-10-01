#!/usr/bin/env python3
"""
Generate transactions and insert directly into PostgreSQL database
"""

import psycopg2
import random
import string
import time
from datetime import datetime, timedelta, timezone, date
from typing import Optional, List, Iterable
import csv
import os


# Database connection string
DB_CONNECTION = "postgresql://neondb_owner:npg_3TOQ6hZlyKzB@ep-billowing-cherry-a14lnj3s-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
# DB_CONNECTION = os.getenv("DB_URL_CONNECTION")
# CSV output directory (per-day files)
CSV_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "generated")

# CSV columns order
CSV_COLUMNS = [
    'id', 'trx_type', 'account_number', 'amount', 'debit_credit', 'subheader',
    'detail_information', 'trx_date', 'trx_time', 'currency', 'created_date',
    'created_by', 'updated_date', 'updated_by'
]


def ensure_csv_dir_exists():
    if not os.path.isdir(CSV_OUTPUT_DIR):
        os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)


def get_csv_path_for_date(d: date) -> str:
    ensure_csv_dir_exists()
    filename = f"transactions_raw_{d.strftime('%Y%m%d')}.csv"
    return os.path.join(CSV_OUTPUT_DIR, filename)


def append_transaction_to_daily_csv(transaction_data: dict, d: Optional[date] = None):
    """Append a single transaction row to the per-day CSV file, creating header if new file."""
    if d is None:
        d = date.today()
    csv_path = get_csv_path_for_date(d)
    file_exists = os.path.isfile(csv_path)
    with open(csv_path, mode='a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: transaction_data.get(k) for k in CSV_COLUMNS})


def flush_csv_buffer(rows: List[dict], d: Optional[date] = None) -> int:
    """Write buffered rows to the per-day CSV and clear the buffer. Returns number of rows flushed."""
    if not rows:
        return 0
    if d is None:
        d = date.today()
    csv_path = get_csv_path_for_date(d)
    file_exists = os.path.isfile(csv_path)
    try:
        with open(csv_path, mode='a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            if not file_exists:
                writer.writeheader()
            for r in rows:
                writer.writerow({k: r.get(k) for k in CSV_COLUMNS})
        flushed = len(rows)
        rows.clear()
        return flushed
    except Exception as e:
        print(f"Error flushing CSV buffer: {e}")
        return 0


def generate_id(length: int = 22) -> str:
    alphabet = string.ascii_letters + string.digits + "-"
    return "".join(random.choice(alphabet) for _ in range(length))


def random_account_number() -> str:
    return str(random.randint(10**9, 10**10 - 1))


def format_date_only(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def format_time_only(dt: datetime) -> str:
    return dt.strftime("%H:%M:%S")


def random_name() -> str:
    first_names = [
        "JAMES", "JOHN", "MICHAEL", "ROBERT", "DAVID",
        "MARY", "PATRICIA", "LINDA", "BARBARA", "ELIZABETH",
    ]
    last_names = [
        "SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES",
        "GARCIA", "MILLER", "DAVIS", "RODRIGUEZ", "MARTINEZ",
    ]
    return random.choice(first_names) + " " + random.choice(last_names)


SUPPORTED_BANKS = ["JAGO", "BCA", "MANDIRI", "BNI", "BRI", "PERMATA", "CIMB"]
SUPPORTED_NON_BNI = [b for b in SUPPORTED_BANKS if b != "BNI"]


def round_amount_to_thousands(amount: int) -> int:
    if amount <= 0:
        return 0
    return amount - (amount % 1000)


class BeneficiaryInfo:
    def __init__(self, bank: str, name: str, account: str):
        self.bank = bank
        self.name = name
        self.account = account
        self.subheader = f"{bank} - {name}"
        self.detail_info = account
    
    def get_key(self) -> str:
        return f"{self.subheader}|{self.account}"


class FraudScenarioManager:
    def __init__(self, timezone_offset_minutes: int = 420, repeat_probability: float = 0.15):
        self.timezone = timezone(timedelta(minutes=timezone_offset_minutes))
        self.repeated_targets = {}
        self.beneficiary_pool = {}
        self.repeat_probability = max(0.0, min(1.0, repeat_probability))

    def ensure_in_pool(self, beneficiary: BeneficiaryInfo) -> BeneficiaryInfo:
        key = beneficiary.get_key()
        if key not in self.beneficiary_pool:
            self.beneficiary_pool[key] = beneficiary
        return self.beneficiary_pool[key]

    def create_new_non_bni_beneficiary(self) -> BeneficiaryInfo:
        bank = random.choice(SUPPORTED_NON_BNI)
        name = random_name()
        account = random_account_number()
        return BeneficiaryInfo(bank, name, account)

    def pick_from_pool_or_none(self) -> Optional[BeneficiaryInfo]:
        if self.beneficiary_pool and random.random() < self.repeat_probability:
            beneficiary_key = random.choice(list(self.beneficiary_pool.keys()))
            return self.beneficiary_pool[beneficiary_key]
        return None

    def record_transfer(self, beneficiary: BeneficiaryInfo, timestamp: datetime):
        key = beneficiary.get_key()
        history = self.repeated_targets.setdefault(key, [])
        history.append(timestamp)
        one_hour_ago = timestamp - timedelta(hours=1)
        self.repeated_targets[key] = [t for t in history if t >= one_hour_ago]

    def is_repetition_over_threshold(self, beneficiary: BeneficiaryInfo) -> bool:
        key = beneficiary.get_key()
        history = self.repeated_targets.get(key, [])
        return len(history) > 2

    def get_fraud_statistics(self) -> dict:
        """Get current fraud detection statistics."""
        stats = {
            'total_beneficiaries': len(self.beneficiary_pool),
            'suspicious_beneficiaries': 0,
            'total_transactions_tracked': 0
        }
        
        for key, history in self.repeated_targets.items():
            stats['total_transactions_tracked'] += len(history)
            if len(history) > 2:
                stats['suspicious_beneficiaries'] += 1
        
        return stats


def load_bni_beneficiaries_from_db() -> List[BeneficiaryInfo]:
    """Load BNI beneficiaries from the database."""
    bni_list = []
    try:
        with psycopg2.connect(DB_CONNECTION) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT name, account_number 
                    FROM bronze.data_nasabah_raw 
                    WHERE status = 'OPENED'
                """)
                for row in cur.fetchall():
                    name, account = row
                    bni_list.append(BeneficiaryInfo('BNI', name, account))
    except Exception as e:
        print(f"Warning: Could not load BNI beneficiaries from DB: {e}")
    return bni_list


def load_sender_accounts_from_db() -> List[str]:
    """Load all account numbers from data_nasabah_raw to use as sender accounts."""
    accounts = []
    try:
        with psycopg2.connect(DB_CONNECTION) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT account_number 
                    FROM bronze.data_nasabah_raw 
                    WHERE status = 'OPENED'
                """)
                for row in cur.fetchall():
                    accounts.append(row[0])
    except Exception as e:
        print(f"Warning: Could not load sender accounts from DB: {e}")
        # Fallback to random accounts if DB fails
        accounts = [random_account_number() for _ in range(200)]
    return accounts


def _batch_insert_transactions_to_db(rows: Iterable[dict]):
    """Batch insert transactions using ON CONFLICT DO NOTHING for id deduplication."""
    from psycopg2.extras import execute_values
    insert_sql = (
        "INSERT INTO bronze.transactions_raw ("
        "id, trx_type, account_number, amount, debit_credit, subheader, "
        "detail_information, trx_date, trx_time, currency, created_date, "
        "created_by, updated_date, updated_by"
        ") VALUES %s ON CONFLICT (id) DO NOTHING"
    )
    values_template = "(" + ",".join(["%s"] * len(CSV_COLUMNS)) + ")"
    try:
        with psycopg2.connect(DB_CONNECTION) as conn:
            with conn.cursor() as cur:
                data = [
                    (
                        r['id'], r['trx_type'], r['account_number'], r['amount'], r['debit_credit'],
                        r['subheader'], r['detail_information'], r['trx_date'], r['trx_time'], r['currency'],
                        r['created_date'], r['created_by'], r['updated_date'], r['updated_by']
                    ) for r in rows
                ]
                if not data:
                    return 0
                execute_values(cur, insert_sql, data, template=values_template, page_size=1000)
            conn.commit()
        return len(data)
    except Exception as e:
        print(f"Error batch inserting transactions: {e}")
        return 0


def insert_single_transaction_to_db(transaction_data: dict) -> int:
    """Insert a single transaction immediately (realtime). Returns 1 if attempted."""
    sql = (
        "INSERT INTO bronze.transactions_raw ("
        "id, trx_type, account_number, amount, debit_credit, subheader, "
        "detail_information, trx_date, trx_time, currency, created_date, "
        "created_by, updated_date, updated_by) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (id) DO NOTHING"
    )
    try:
        with psycopg2.connect(DB_CONNECTION) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    transaction_data['id'],
                    transaction_data['trx_type'],
                    transaction_data['account_number'],
                    transaction_data['amount'],
                    transaction_data['debit_credit'],
                    transaction_data['subheader'],
                    transaction_data['detail_information'],
                    transaction_data['trx_date'],
                    transaction_data['trx_time'],
                    transaction_data['currency'],
                    transaction_data['created_date'],
                    transaction_data['created_by'],
                    transaction_data['updated_date'],
                    transaction_data['updated_by'],
                ))
                conn.commit()
        return 1
    except Exception as e:
        print(f"Error realtime insert: {e}")
        return 0


def ingest_daily_csv_to_db(d: Optional[date] = None) -> int:
    """
    Ingest the per-day CSV file into bronze.transactions_raw, skipping existing ids.
    Returns number of rows attempted (existing rows are skipped by ON CONFLICT).
    """
    if d is None:
        d = date.today()
    csv_path = get_csv_path_for_date(d)
    if not os.path.isfile(csv_path):
        print(f"No CSV found to ingest for {d.isoformat()} at {csv_path}")
        return 0
    inserted = 0
    
    def _fetch_existing_ids(ids: List[str]) -> set:
        if not ids:
            return set()
        existing: set = set()
        try:
            with psycopg2.connect(DB_CONNECTION) as conn:
                with conn.cursor() as cur:
                    step = 1000
                    for i in range(0, len(ids), step):
                        chunk = ids[i:i+step]
                        # Build safe IN list
                        in_list = ",".join(["'" + str(x).replace("'", "''") + "'" for x in chunk])
                        cur.execute(
                            f"SELECT id FROM bronze.transactions_raw WHERE id IN ({in_list})"
                        )
                        rows = cur.fetchall()
                        if rows:
                            existing.update([str(r[0]) for r in rows])
        except Exception as e:
            print(f"Warning: could not pre-check existing ids: {e}")
        return existing
    try:
        with open(csv_path, mode='r', newline='') as f:
            reader = csv.DictReader(f)
            buffer: List[dict] = []
            for row in reader:
                buffer.append(row)
                if len(buffer) >= 5000:
                    ids = [str(r['id']) for r in buffer if r.get('id')]
                    existing_ids = _fetch_existing_ids(ids)
                    to_insert = [r for r in buffer if str(r.get('id')) not in existing_ids]
                    inserted += _batch_insert_transactions_to_db(to_insert)
                    buffer = []
            if buffer:
                ids = [str(r['id']) for r in buffer if r.get('id')]
                existing_ids = _fetch_existing_ids(ids)
                to_insert = [r for r in buffer if str(r.get('id')) not in existing_ids]
                inserted += _batch_insert_transactions_to_db(to_insert)
        print(f"Ingested CSV {os.path.basename(csv_path)} -> inserted {inserted} new row(s); existing ids skipped")
        return inserted
    except Exception as e:
        print(f"Error ingesting CSV {csv_path}: {e}")
        return 0


def run_generator_to_db(
    rate_per_minute: int = 60,
    timezone_offset_minutes: int = 420,
    fraud_ratio: float = 0.05,
    repeat_probability: float = 0.15,
    max_transactions: Optional[int] = None,
    csv_batch_size: int = 1,
    realtime_db: bool = True
):
    """
    Generate transactions and append them to a per-day CSV. Use ingest_daily_csv_to_db()
    to load the CSV into the database with deduplication.
    """
    
    tz = timezone(timedelta(minutes=timezone_offset_minutes))
    fraud = FraudScenarioManager(
        timezone_offset_minutes=timezone_offset_minutes,
        repeat_probability=repeat_probability,
    )

    # Load BNI beneficiaries from database
    bni_beneficiaries = load_bni_beneficiaries_from_db()
    print(f"Loaded {len(bni_beneficiaries)} BNI beneficiaries from database")

    # Load sender accounts from database
    sender_accounts = load_sender_accounts_from_db()
    print(f"Loaded {len(sender_accounts)} sender accounts from database")
    sleep_seconds = max(0.0, 60.0 / max(1, rate_per_minute))
    
    transaction_count = 0
    csv_buffer: List[dict] = []
    
    print(f"🚀 Starting transaction generation to CSV...")
    print(f"Rate: {rate_per_minute} transactions/minute")
    print(f"Fraud ratio: {fraud_ratio}")
    print(f"Press Ctrl+C to stop")
    
    try:
        while True:
            if max_transactions and transaction_count >= max_transactions:
                break
                
            now = datetime.now(tz=tz)
            account_number = random.choice(sender_accounts)

            # Decide whether to repeat to an existing beneficiary (fraud pattern)
            beneficiary: Optional[BeneficiaryInfo] = None
            participate_fraud = random.random() < fraud_ratio
            if participate_fraud:
                beneficiary = fraud.pick_from_pool_or_none()

            # If not repeating, choose destination: BNI (from DB) or non-BNI (random)
            if beneficiary is None:
                is_bni_destination = bool(bni_beneficiaries) and (random.random() < 0.5)
                if is_bni_destination:
                    beneficiary = random.choice(bni_beneficiaries)
                    beneficiary = fraud.ensure_in_pool(beneficiary)
                else:
                    beneficiary = fraud.ensure_in_pool(fraud.create_new_non_bni_beneficiary())

            # Amount generation
            amount = random.choice([
                random.randint(10_000, 200_000),
                random.randint(201_000, 1_000_000),
            ])
            if participate_fraud:
                amount = random.randint(201_000, 5_000_000)
            amount = round_amount_to_thousands(amount)

            fraud.record_transfer(beneficiary, now)
            is_repeated = fraud.is_repetition_over_threshold(beneficiary)

            # Prepare transaction data
            transaction_data = {
                'id': generate_id(),
                'trx_type': 'Transfer',
                'account_number': account_number,
                'amount': amount,
                'debit_credit': 'D',
                'subheader': beneficiary.subheader,
                'detail_information': beneficiary.detail_info,
                'trx_date': format_date_only(now),
                'trx_time': format_time_only(now),
                'currency': 'IDR',
                'created_date': now.strftime("%Y-%m-%d %H:%M:%S"),
                'created_by': 'SYSTEM',
                'updated_date': now.strftime("%Y-%m-%d %H:%M:%S"),
                'updated_by': 'SYSTEM'
            }

            # CSV write (realtime or batched)
            if max(1, csv_batch_size) == 1:
                append_transaction_to_daily_csv(transaction_data)
            else:
                csv_buffer.append(transaction_data)
                if len(csv_buffer) >= max(1, csv_batch_size):
                    flushed = flush_csv_buffer(csv_buffer)
                    if flushed:
                        print(f"Flushed {flushed} rows to daily CSV")

            # Realtime DB insert
            if realtime_db:
                insert_single_transaction_to_db(transaction_data)
            transaction_count += 1

            # Print progress
            if transaction_count % 100 == 0:
                stats = fraud.get_fraud_statistics()
                print(f"Generated {transaction_count} transactions - "
                      f"Beneficiaries: {stats['total_beneficiaries']}, "
                      f"Suspicious: {stats['suspicious_beneficiaries']}")
                
                if is_repeated and amount > 200000:
                    print(f"[ALERT] Potential scam: Transfers to {beneficiary.subheader} "
                          f"({beneficiary.detail_info}) - Amount: {amount:,} IDR")

            time.sleep(sleep_seconds)
            
    except KeyboardInterrupt:
        # Flush remaining CSV buffer if any
        if csv_buffer:
            flushed = flush_csv_buffer(csv_buffer)
            if flushed:
                print(f"Flushed remaining {flushed} rows to daily CSV")
        print(f"\n🛑 Stopped by user. Generated {transaction_count} transactions total.")
        print("Tip: Run ingest_daily_csv_to_db() to insert today's CSV into DB (skip duplicates).")
    except Exception as e:
        print(f"❌ Error: {e}")


def main():
    """Main function with command line arguments."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate transactions to CSV and optionally realtime DB")
    parser.add_argument("--rate", type=int, default=60, help="Transactions per minute")
    parser.add_argument("--fraud-ratio", type=float, default=0.25, help="Fraud participation ratio")
    parser.add_argument("--repeat-prob", type=float, default=0.15, help="Repeat beneficiary probability")
    parser.add_argument("--max-transactions", type=int, help="Maximum transactions to generate")
    parser.add_argument("--csv-batch-size", type=int, default=1, help="CSV write batch size (1 = realtime)")
    parser.add_argument("--no-realtime-db", action='store_true', help="Disable realtime DB insert")
    
    args = parser.parse_args()
    
    run_generator_to_db(
        rate_per_minute=args.rate,
        fraud_ratio=args.fraud_ratio,
        repeat_probability=args.repeat_prob,
        max_transactions=args.max_transactions,
        csv_batch_size=args.csv_batch_size,
        realtime_db=not args.no_realtime_db
    )


if __name__ == "__main__":
    main()
