from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import gcsfs
import json
from google.cloud import bigquery
import pandas_gbq
from datetime import datetime

#credentials Blank-Space.io
credentialsBlankSpace = service_account.Credentials.from_service_account_file(
'/home/airflow/gcs/data/GCP/pkl-playing-fields-7314d23dc2d0.json')
project_idBlankSpace = 'pkl-playing-fields'
clientBlankSpace = bigquery.Client(credentials= credentialsBlankSpace,project=project_idBlankSpace)

fileMyJsonCre = '/home/airflow/gcs/data/GCP/silent-album-311314-f6e80853908a.json'
#fileMyJsonCre = 'D:/BlankSpace.io/Code BlankSpace.io/GCP/GCP/silent-album-311314-f6e80853908a.json'
myProjectId = 'silent-album-311314'
myTableGCP = "SumTransaction.TransactionTemp"
credentialsMyGcp = service_account.Credentials.from_service_account_file(fileMyJsonCre)
client2 = bigquery.Client(credentials= credentialsMyGcp,project=myProjectId)

def ReadBigQuery():

    global newdf 

    sql = """
            SELECT *
            FROM pkl-playing-fields.unified_events.event 
            where event_name = 'purchase_item'
            """
    query = clientBlankSpace.query(sql)
    results = query.result()

    df = results.to_dataframe()

    df.info()
    
    #df.to_json('D:/BlankSpace.io/Code BlankSpace.io/GCP/GCP/result.json', orient='records', lines=True)
    result = df.to_json(orient="records",date_format="iso")
    parsed = json.loads(result)

    newdf = pd.DataFrame(columns=['transaction_id','transaction_detail_id','transaction_number', \
            'transaction_datetime','purchase_quantity','purchase_amount','purchase_payment_method', \
            'purchase_source','product_id','user_id','state','city','created_at' \
            ])

    for i in range (0,len(parsed)):

            transaction_datetime = parsed[i]['event_datetime']
            print(transaction_datetime)
            user_id = parsed[i]['user_id']
            print(user_id)
            state = parsed[i]['state']
            print(state)
            city = parsed[i]['city']
            print(city)
            created_at = parsed[i]['created_at']
            print(created_at)

            parsed[0]['event_params']
            df2 =pd.DataFrame.from_dict(parsed[0]['event_params'])
            result2 = df2.to_json(orient="records")
            parsed2 = json.loads(result2)

            for i in range (0, len(parsed2)):
                    if (parsed2[i]['key'] == 'transaction_id') or (parsed2[i]['key'] == 'transaction_detail_id') \
                    or (parsed2[i]['key'] == 'transaction_number') or (parsed2[i]['key'] == 'purchase_quantity') \
                    or (parsed2[i]['key'] == 'purchase_amount') or (parsed2[i]['key'] == 'purchase_payment_method') \
                    or (parsed2[i]['key'] == 'purchase_source') or (parsed2[i]['key'] == 'product_id'):
                            print(parsed2[i]['key'])
                            if parsed2[i]['value'] is None: 
                                    values = ''
                            elif parsed2[i]['key'] is not None:
                                    if parsed2[i]['value']['float_value'] is not None:
                                            values = parsed2[i]['value']['float_value']
                                    elif parsed2[i]['value']['int_value'] is not None:
                                            values = parsed2[i]['value']['int_value']
                                    elif parsed2[i]['value']['bool_value'] is not None:
                                            values = parsed2[i]['value']['bool_value']
                                    elif parsed2[i]['value']['string_value'] is not None:
                                            values = parsed2[i]['value']['string_value']
                            print(values)
                            if parsed2[i]['key'] == 'transaction_id':
                                    transaction_id = values
                            elif parsed2[i]['key'] == 'transaction_detail_id':
                                    transaction_detail_id = values
                            elif parsed2[i]['key'] == 'transaction_number':
                                    transaction_number = values
                            elif parsed2[i]['key'] == 'purchase_quantity':
                                    purchase_quantity = values
                            elif parsed2[i]['key'] == 'purchase_amount':
                                    purchase_amount = values
                            elif parsed2[i]['key'] == 'purchase_payment_method':
                                    purchase_payment_method = values
                            elif parsed2[i]['key'] == 'purchase_source':
                                    purchase_source = values
                            elif parsed2[i]['key'] == 'product_id':
                                    product_id = values   

            new_row = {'transaction_id': transaction_id, \
                    'transaction_detail_id':transaction_detail_id, \
                    'transaction_number': transaction_number, \
                    'transaction_datetime' : transaction_datetime, \
                    'purchase_quantity':purchase_quantity, \
                    'purchase_amount': purchase_amount, \
                    'purchase_payment_method':purchase_payment_method, \
                    'purchase_source': purchase_source, \
                    'product_id':product_id, \
                    'user_id':user_id, \
                    'state':state, \
                    'city':city, \
                    'created_at':created_at, \
                    'ext_created_at':datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
            newdf = newdf.append(new_row, ignore_index=True)

            newdf['transaction_id'] = pd.to_numeric(newdf['transaction_id'],downcast ='signed')
            newdf['transaction_detail_id'] = pd.to_numeric(newdf['transaction_detail_id'],downcast ='signed')
            newdf['purchase_quantity'] = pd.to_numeric(newdf['purchase_quantity'],downcast ='signed')
            newdf['product_id'] = pd.to_numeric(newdf['product_id'],downcast ='signed')
            newdf['user_id'] = pd.to_numeric(newdf['user_id'],downcast ='signed')

def InsertToTable():
    pandas_gbq.to_gbq(newdf, myTableGCP, project_id=myProjectId,if_exists='replace', credentials=credentialsMyGcp)

def DeleteData():

    sql = "delete FROM silent-album-311314.SumTransaction.Transaction where user_id is not null "
    query = client2.query(sql)
    results = query.result()

def MoveData():
   
    sql = """
    insert into silent-album-311314.SumTransaction.Transaction 
    (transaction_id,transaction_detail_id,transaction_number,transaction_datetime,purchase_quantity,purchase_amount,
    purchase_payment_method,purchase_source,product_id,user_id,state,city,created_at,ext_created_at
    )
    select transaction_id,transaction_detail_id,transaction_number,transaction_datetime,purchase_quantity,purchase_amount,
    purchase_payment_method,purchase_source,product_id,user_id,state,city,created_at,ext_created_at
    from silent-album-311314.SumTransaction.TransactionTemp
    """
    query = client2.query(sql)
    results = query.result()

if __name__ == '__main__':
    ReadBigQuery()
    pd.set_option('display.max_columns', None)
    InsertToTable()
    DeleteData()
    MoveData()
