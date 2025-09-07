import pandas as pd
from databaseConfig import engine



def getDataNasabahRaw(schema_table, table):
    """
    Get Data from schema bronze data nasabah
    """
    print("Get Data Nasabah Raw")
    try: 
        query = f"""
                select "name", account_number, phone_number, status from {schema_table}.{table}
                """
        df = pd.read_sql_query(query, engine)
        
        list_nasabah = df.to_dict(orient='records')
        return list_nasabah
    except Exception as e:
        print(f"Error with description: {e}")
        return None

# print(json.dumps(getDataNasabahRaw))

def getDataTransactionRaw(schema_table, table):
    """
    Get Data from schema bronze transaction_raw
    """
    print("Get Transaction Raw")

    try:
        query = f"""
                select 
                id, 
                trx_type , 
                account_number , 
                amount, 
                debit_credit, 
                subheader,
                detail_information,
                trx_date ,
                trx_time ,
                currency 
                from {schema_table}.{table} order by trx_date, trx_time asc

                """
        df = pd.read_sql_query(query, engine)
        list_transaction_raw = df.to_dict(orient='records')
        return list_transaction_raw
    except Exception as e:
        print(f"Error with description: {e}")
        return None


        
