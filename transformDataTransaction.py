# transformDataTransaction.py

import pandas as pd
from databaseConfig import engine # This import will now work

def get_data_criteria():
    try:
        # Use pandas to read from the 'silver.criteria' table using the engine
        query = "Select code, description, level from silver.criteria"
        # df = pd.read_sql_table('criteria', engine, schema='silver')
        df = pd.read_sql_query(query, engine)

        # print("Successfully loaded data to DataFrame:")
        # print(df.head())
        return df
    except Exception as e:
        print(f"Error with description: {e}")
        return None