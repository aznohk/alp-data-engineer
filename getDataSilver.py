import pandas as pd
from databaseConfig import engine

def getDataCriteria(schema_table, table):

    """
    Get Data Silver Criteria
    """

    print("Get Data Criteria")

    try:
        query = f"""
                select code, description, level from {schema_table}.{table}
                """
        
        df = pd.read_sql_query(query, engine)
        list_criteria = df.to_dict(orient='records')

        return list_criteria
    except Exception as e:
        print(f"Error with description : {e}")
        return None