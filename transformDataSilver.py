import pandas as pd
from databaseConfig import engine
from collections import defaultdict
import datetime
import uuid

def transformDataSilver(list_trx, list_nasabah, list_criteria):
    """
    Validate Scam Transaction
    
    """
    grouped_data = defaultdict(list)

    for trx in list_trx:
        accountNum = trx['account_number']
        nasabah_from_obj = next(nasabah for nasabah in list_nasabah if nasabah["account_number"] == accountNum)
        isOpenNasabahFrom = nasabah_from_obj["status"] == "opened".upper() or nasabah_from_obj["status"] == "blocked".upper()
        accountTo = trx['detail_information']
        
        headerInfo = trx['subheader']
        bankTo = headerInfo.split(" - ")
        isBni = bankTo == "BNI"
        isOpenAccountTo = False
        if isBni:
            nasabah_to_obj = next(nasabahTo for nasabahTo in list_nasabah if nasabahTo["account_number"]==accountTo)
            isOpenAccountTo = nasabah_to_obj["status"] == "opened".upper() or nasabah_to_obj["status"] == "blocked".upper()
            if isOpenNasabahFrom and isOpenAccountTo:
                trx['status_trx'] = "SUCCESS"
            else:
                trx['status_trx'] = "FAILED"
        else :
            if isOpenNasabahFrom:
                trx['status_trx'] = "SUCCESS"
            else:
                trx['status_trx'] = "FAILED"
        trx['criteria_anomali'] = '0'
        obj_criteria_normal = next(criteria for criteria in list_criteria if criteria["code"] ==  trx['criteria_anomali'])
        trx['description_anomali'] = obj_criteria_normal["description"]

        key = (trx['account_number'], trx['detail_information'])
        grouped_data[key].append(trx)

    for key, transactions in grouped_data.items():
        transactions.sort(key=lambda x: (x['trx_date'], x['trx_time']))

        for i in range(len(transactions) - 1):
            count = 0
            trx1 = transactions[i]
            trx2 = transactions[i+1]
            # dt1 = datetime.datetime.combine(datetime.datetime.strptime(trx1['trx_date'].strftime('%Y-%M-%d')).date(), 
            #                                 datetime.datetime.strptime(trx1['trx_time'].strftime('%H:%M:%S')).time())
            # dt2 = datetime.datetime.combine(datetime.datetime.strptime(trx2['trx_date'].strftime('%Y-%M-%d')).date(), 
            #                                 datetime.datetime.strptime(trx2['trx_time'].strftime('%H:%M:%S')).time())

            dt1 = datetime.datetime.combine(trx1['trx_date'], trx1['trx_time'])
            dt2 = datetime.datetime.combine(trx2['trx_date'], trx2['trx_time'])

            time_dif = (dt2-dt1).total_seconds()
            # selisih = time_dif.total_seconds()

            if time_dif < 3600:
                if trx2['amount'] > trx1['amount']:
                    trx1['criteria_anomali'] = '1'
                    trx2['criteria_anomali'] = '1'
                    obj_criteria = next(criteria for criteria in list_criteria if criteria["code"] == "1")
                    trx1['description_anomali'] = obj_criteria["description"]
                    trx2['description_anomali'] = obj_criteria["description"]
                    # count += 1
    
    
    return mappingData(list_trx)

def mappingData(transactions):
    result = []
    for trx in transactions:
        data = {
            'id' : uuid.uuid4(),
            'account_num': trx['account_number'],
            'amount': trx['amount'],
            'currency': trx['currency'],
            'trx_type': trx['debit_credit'],
            'account_to': trx['detail_information'],
            'narrative': trx['trx_type'] + " ke " + trx['subheader'],
            'status_trx': trx['status_trx'],
            'trx_date': trx['trx_date'],
            'trx_time': trx['trx_time'],
            'criteria_anomali': trx['criteria_anomali'],
            'description_anomali': trx['description_anomali'],
            'code_transaction': "TRX:"+trx['id'],
            'created_by': "SYSTEM",
            'created_date': datetime.datetime.now(),
            'updated_by': "SYSTEM",
            'updated_date': datetime.datetime.now()
        }
        result.append(data)
    
    return result

def inserDataTransaction(list_trx):

    df_new = pd.DataFrame(list_trx)
   
    try:
        existings_code_trx = pd.read_sql_query("SELECT code_transaction from silver.transactions", engine)
        existings_code_trx_set = set(existings_code_trx['code_transaction'])

        df_to_insert = df_new[~df_new['code_transaction'].isin(existings_code_trx_set)].copy()

        if df_to_insert.empty:
            print("Tidak ada data yang di Insert")
            return df_new
        
        row_inserted = df_to_insert.to_sql(
            schema='silver',
            name='transactions',
            con=engine,
            if_exists='append',
            index=False
        )

        print(f"Successfully inserted : {row_inserted}")
        return df_to_insert
    except Exception as e:
        print(f"Error with description : {e}")
        return pd.DataFrame()
    