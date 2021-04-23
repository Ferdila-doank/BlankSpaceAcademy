import pandas as pd
import gcsfs
from google.oauth2 import service_account
import json
from google.cloud import bigquery
import pandas_gbq
from datetime import datetime

# My credential project and project id 
fileMyJsonCre = '/home/airflow/gcs/data/GCP/silent-album-311314-f6e80853908a.json'
myProjectId = 'silent-album-311314'
myTableGCP = "Browsing.KeywordTemp"

# Blankspace credential project and project id 
fileBlankSpaceCre = 'pkl-playing-fields-7314d23dc2d0.json'
blankSpaceProjectId = 'pkl-playing-fields'

credentialsMyGcp = service_account.Credentials.from_service_account_file(fileMyJsonCre)
client = bigquery.Client(credentials= credentialsMyGcp,project=myProjectId)

#date = '20210315'

def ReadConvertCsv(csvdate):

    global df, date 

    #Uncoment this code if you want process by date 
    #date = datetime.now().strftime('%Y%m%d')
    #date = '20210314'
    date = csvdate 
    csvFile = 'asia-south1-asia-south1-54a4ba89-bucket/keyword_search/keyword_search_search_' + str(date) + '.csv'
    #csvFile = 'asia-south1-asia-south1-54a4ba89-bucket/keyword_search/keyword_search_search_20210311.csv'

    fs = gcsfs.GCSFileSystem(project=myProjectId,token=fileMyJsonCre)
    with fs.open(csvFile) as f:
        df = pd.read_csv(f)
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['created_at'] = pd.to_datetime(df['created_at']).dt.date
        df['created_at'] = pd.to_datetime(df['created_at'])
        print(df.info())
        print(df.head())
        print('Total rows in csv: ' + str(df.shape[0]))

def InsertToTable():
    pandas_gbq.to_gbq(df, myTableGCP, project_id=myProjectId,if_exists='replace', credentials=credentialsMyGcp)

def MoveData():
   
    sql = """
    insert into silent-album-311314.Browsing.Keyword (user_id,search_keyword,search_result_count,created_at)
    select user_id,search_keyword,search_result_count,created_at 
    from silent-album-311314.Browsing.KeywordTemp
    where user_id not in (select user_id from silent-album-311314.Browsing.Keyword)
    """
    query = client.query(sql)
    results = query.result()

def DeleteDataTemp():

    sql = "delete FROM silent-album-311314.Browsing.Keyword " \
          " where cast (created_at AS STRING) like '" + str(date)[0:4] \
          + "-" + str(date)[4:6]  + "-" + str(date)[6:9] + "%'"

    query = client.query(sql)
    results = query.result()

def GetMaxSearch():
    global created_at, search_keyword,max_search

    sql = "select sum(search_result_count) as max_search, search_keyword, created_at " \
          "from silent-album-311314.Browsing.Keyword " \
          "where cast (created_at AS STRING) like '" + str(date)[0:4] \
          + "-" + str(date)[4:6]  + "-" + str(date)[6:9] + "%'" \
          "group by search_keyword,created_at " \
          "order by max_search desc " \
          "limit 1"
    query = client.query(sql)
    results = query.result()

    pdMax = results.to_dataframe()
    print(pdMax.head())
    created_at = pdMax['created_at'][0]
    search_keyword = pdMax['search_keyword'][0]
    max_search = pdMax['max_search'][0]
    print(created_at)

def InsertMaxSearch():
   
    sql = "insert into silent-album-311314.Browsing.MostSearch (created_at,search_keyword,search_result_count)" \
          "values ('" +  str(created_at)  + "','" + str(search_keyword) + "'," + str(max_search) + ")"

    query = client.query(sql)
    results = query.result()

def DeleteMaxSearch():

    sql = "delete FROM silent-album-311314.Browsing.MostSearch " \
          " where cast (created_at AS STRING) like '" + str(date)[0:4] \
          + "-" + str(date)[4:6]  + "-" + str(date)[6:9] + "%'"

    query = client.query(sql)
    results = query.result()

if __name__ == '__main__':
    listdate = ['20210310','20210311','20210312','20210313','20210314','20210315']

    for i in listdate:
        print('process date : ' + str(i))
        ReadConvertCsv(csvdate=i)
        InsertToTable()
        DeleteDataTemp()
        MoveData()
        DeleteMaxSearch()
        GetMaxSearch()
        InsertMaxSearch()
