from os import listdir
from os.path import isfile, join
import base64
import datetime
import shutil
import datetime

from google.cloud import storage
from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account

import pandas as pd

fileMyJsonCre = 'pelagic-campus-312012-12c4463aaf3b.json'
client = storage.Client.from_service_account_json(json_credentials_path=fileMyJsonCre)

PATH = "D:/BlankSpace.io/Code BlankSpace.io/DataProc/Json/"
NewPATH = "D:/BlankSpace.io/Code BlankSpace.io/DataProc/" 

def UploadFileToGCP():
  date = datetime.datetime.now()
  strdate = datetime.datetime.now().strftime('%Y-%m-%d')

  newDateFile = datetime.datetime.strptime(strdate, '%Y-%m-%d') 
  newDateFile += datetime.timedelta(days=23)

  filePath = PATH + str(date.strftime('%Y-%m-%d')) + '.json'

  if isfile(filePath):
    newFile = NewPATH + newDateFile.strftime('%Y-%m-%d') + '.json'
    print('Copy Data from : ' + newFile)
    shutil.copyfile(filePath,newFile)

    df = pd.read_json(newFile, lines=True)
    flight_date = df['flight_date'].unique()[0]
    flight_date_new = datetime.datetime.strptime(flight_date, '%Y-%m-%d')
    flight_date_new += datetime.timedelta(days=23)
    flight_date_new = str(flight_date_new.strftime('%Y-%m-%d'))
    df.loc[:,'flight_date'] = flight_date_new

    df.to_json(newFile, orient='records', lines=True)

    bucket = client.bucket('week_3_bucket')

    fileName = 'Data/' + newDateFile.strftime('%Y-%m-%d') + '.json'
    object_name_in_gcs_bucket = bucket.blob(fileName)

    object_name_in_gcs_bucket.upload_from_filename(newDateFile.strftime('%Y-%m-%d') + '.json')

  else:
    print ('File ' + str(filePath) + 'Not Found')

if __name__ == '__main__':
  UploadFileToGCP()