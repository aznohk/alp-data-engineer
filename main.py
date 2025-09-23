from databaseConfig import get_db_session
from sqlalchemy import text
from transformDataTransaction import get_data_criteria
from getDataBronze import getDataNasabahRaw, getDataTransactionRaw
from getDataSilver import getDataCriteria
from transformDataSilver import transformDataSilver, inserDataTransaction
import os
from pathlib import Path
import pandas as pd
import time
import datetime
from dotenv import load_dotenv


def my_database_connection(db_session):
    if not db_session:
        print("No Database available")
        return
    
    try:
        result = db_session.execute(text("SELECT version();")).scalar()
        print(f"Database version: {result}")
        print("Database successfully connection")
    except Exception as e:
        print(f"Error during database connection: {e}")
    finally:
        pass

def main():
    start_time = time.time()
    db_session = get_db_session()
    if db_session:
        try:
            my_database_connection(db_session)

            list_nasabah_raw = getDataNasabahRaw('bronze', 'data_nasabah_raw')
            list_trx_raw = getDataTransactionRaw('bronze', 'transactions_raw')
            criterias = getDataCriteria('silver', 'criteria')
            if list_nasabah_raw is not None and list_trx_raw is not None and criterias is not None :
                # print("\n--- DataFrame Summary ---")
                # print(user_df.info())
                # print("\n--- First 5 rows ---")
                # print(user_df.head())
                # print(json.dumps(user_df))
                # for row in user_df:
                #     print(row)
                list_trx_transform = transformDataSilver(list_trx=list_trx_raw, list_nasabah=list_nasabah_raw, list_criteria=criterias)
                data_inserted = inserDataTransaction(list_trx=list_trx_transform)
                if not data_inserted.empty:
                    now = datetime.datetime.now()
                    timestamp_str = now.strftime("%Y%m%d_%H%M%S")
                    fileName = f"silver_transactions_{timestamp_str}.csv"
                    load_dotenv()
                    # Resolve output directory from env or default to ./generated next to this file
                    file_path = os.getenv("PATH_FILE_GENERATED")
                    if not file_path or not isinstance(file_path, str):
                        file_path = str((Path(__file__).resolve().parent / "generated"))
                    os.makedirs(file_path, exist_ok=True)
                    full_file_path = os.path.join(file_path, fileName)
                    data_inserted.to_csv(f"{full_file_path}", index=False)
                    print(f"Successfully created {fileName}")
                # df = pd.DataFrame(list_trx_transform)
                # df.to_csv('silver_transaction.csv', index=False)
                else:
                    print(f"Nothing data can be generated")

            else:
                print("\nFailed to get DataFrame. Check for errors.")

        finally:
            db_session.close()
            print("Database session closed")
            end_time = time.time()
            duration = end_time - start_time
            print(f"Execution time : {duration:.4f} seconds")
    else:
        print("Could not get a database session. Exiting")

if __name__== "__main__":
    main()