import pandas as pd
from databaseConfig import engine
import datetime

def transformDataGold():
    df = pd.read_sql_query("SELECT * FROM silver.transactions", engine)
    if df.empty:
        print("Tidak ada data di silver.transactions")
        return [], [], []

    df['trx_date'] = pd.to_datetime(df['trx_date']).dt.date
    df['criteria_anomali'] = df['criteria_anomali'].astype(int)
    now = datetime.datetime.now()

    # Kolom debit/credit
    df['debit_amount'] = df['amount'].where(df['trx_type'] == 'D', 0)
    df['credit_amount'] = df['amount'].where(df['trx_type'] == 'C', 0)

    # Pisahkan normal dan abnormal
    df_normal = df[df['criteria_anomali'] == 0].copy()
    df_abnormal = df[df['criteria_anomali'] != 0].copy()
    print("Jumlah normal:", len(df_normal))
    print("Jumlah abnormal:", len(df_abnormal))

    # --- Detail Transactions ---
    def prepare_gold(df_subset):
        if df_subset.empty:
            return df_subset
        df_subset = df_subset.copy()
        df_subset['failed_trx'] = (df_subset['status_trx'] == 'FAILED').astype(int)
        df_subset['anomaly_trx'] = (df_subset['criteria_anomali'] != 0).astype(int)
        df_subset['total_trx'] = 1
        df_subset['total_amount'] = df_subset['amount']
        df_subset['total_debit'] = df_subset['debit_amount']
        df_subset['total_credit'] = df_subset['credit_amount']
        df_subset['created_by'] = "SYSTEM"
        df_subset['created_date'] = now
        df_subset['updated_by'] = "SYSTEM"
        df_subset['updated_date'] = now
        return df_subset

    normal_gold = prepare_gold(df_normal)
    abnormal_gold = prepare_gold(df_abnormal)
    list_normal = normal_gold.to_dict(orient='records')
    list_abnormal = abnormal_gold.to_dict(orient='records')

    # --- Summary per trx_date per tipe_anomali ---
    def prepare_summary_daily(normal_df, abnormal_df):
        summary_list = []

        for df_subset, tipe_anomali in [(normal_df, 'Normal'), (abnormal_df, 'Abnormal')]:
            if df_subset.empty:
                continue
            grouped = df_subset.groupby('trx_date').agg(
                transaksi_success=('total_trx', 'sum'),
                transaksi_failed=('failed_trx', 'sum'),
                total_amount_transaksi=('total_amount', 'sum'),
                total_transaksi=('total_trx', 'sum')
            ).reset_index()
            grouped['tipe_anomali'] = tipe_anomali
            grouped['level_anomali'] = 'Low' if tipe_anomali == 'Normal' else 'High'
            grouped['currency'] = 'IDR'
            grouped['created_by'] = 'Python Script'
            grouped['created_date'] = now
            summary_list.extend(grouped.to_dict(orient='records'))

        return summary_list

    summary_all = prepare_summary_daily(normal_gold, abnormal_gold)

    return list_normal, list_abnormal, summary_all


def insertDataGold(list_data, table_name):
    if not list_data:
        print(f"Tidak ada data untuk tabel {table_name}")
        return

    df_new = pd.DataFrame(list_data)

    if table_name == 'transactions_summary':
        columns_gold = [
            'trx_date','tipe_anomali','level_anomali','transaksi_success',
            'transaksi_failed','total_amount_transaksi','total_transaksi',
            'currency','created_by','created_date'
        ]
    else:
        columns_gold = [
            'id','account_num','trx_date','total_trx','total_amount',
            'total_debit','total_credit','failed_trx','anomaly_trx',
            'created_by','created_date','updated_by','updated_date'
        ]

    df_new = df_new[columns_gold]

    try:
        df_new.to_sql(
            schema='gold',
            name=table_name,
            con=engine,
            if_exists='append',
            index=False
        )
        print(f"Successfully inserted to {table_name}: {len(df_new)} rows")
    except Exception as e:
        print(f"Error insert {table_name}: {e}")


if __name__ == "__main__":
    normal_data, abnormal_data, summary_data = transformDataGold()
    insertDataGold(normal_data, "transactions_normal")
    insertDataGold(abnormal_data, "transactions_abnormal")
    insertDataGold(summary_data, "transactions_summary")
