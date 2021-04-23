from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import gcsfs
import json
from google.cloud import bigquery
import pandas_gbq
from datetime import datetime

# My credential project and project id 
#fileMyJsonCre = 'D:\BlankSpace.io\Code BlankSpace.io\GCP\GCP\silent-album-311314-f6e80853908a.json'
#fileMyJsonCre = 'silent-album-311314-f6e80853908a.json'

fileMyJsonCre = '/home/airflow/gcs/data/GCP/silent-album-311314-f6e80853908a.json'
myProjectId = 'silent-album-311314'
myTableGCP = "SumTransaction.TransactionTemp"
credentialsMyGcp = service_account.Credentials.from_service_account_file(fileMyJsonCre)
client2 = bigquery.Client(credentials= credentialsMyGcp,project=myProjectId)


#Credential Blank-space
#credentials = service_account.Credentials.from_service_account_file('pkl-playing-fields-7314d23dc2d0.json')

credentials = service_account.Credentials.from_service_account_file('/home/airflow/gcs/data/GCP/pkl-playing-fields-7314d23dc2d0.json')
project_id = 'pkl-playing-fields'
client = bigquery.Client(credentials= credentials,project=project_id)

def GetFromBucket():
    global newdf
    sql = """
            SELECT * FROM pkl-playing-fields.unified_events.event
            """
    query = client.query(sql)
    results = query.result()

    df = results.to_dataframe()

    countRow = len(df)

    newdf = pd.DataFrame(columns=['user_id','state','city','created_at'])

    for i in range (0,countRow):
        print(str(df['user_id'][i]) + ',' + str(df['state'][i])+ ',' + str(df['city'][i]) + ',' + str(df['created_at'][i]))         
        new_row = {'user_id': df['user_id'][i], \
                    'state': df['state'][i],
                    'city': df['city'][i],
                    'created_at': df['created_at'][i],
                    }

        newdf = newdf.append(new_row, ignore_index=True)
        newdf['user_id'] = pd.to_numeric(df['user_id'])

def InsertToTable():
    credentialsMyGcp = service_account.Credentials.from_service_account_file(fileMyJsonCre)
    client = bigquery.Client(credentials= credentialsMyGcp,project=myProjectId)
    pandas_gbq.to_gbq(newdf, myTableGCP, project_id=myProjectId,if_exists='replace', credentials=credentialsMyGcp)

def DeleteData():

    sql = "delete FROM silent-album-311314.SumTransaction.Transaction where user_id is not null "
    query = client2.query(sql)
    results = query.result()

def MoveData():
   
    sql = """
    insert into silent-album-311314.SumTransaction.Transaction 
    (user_id,state,city,created_at)
    select user_id,state,city,created_at
    from silent-album-311314.SumTransaction.TransactionTemp
    """
    query = client2.query(sql)
    results = query.result()

if __name__ == '__main__':
    GetFromBucket()
    InsertToTable()
    DeleteData()
    MoveData()