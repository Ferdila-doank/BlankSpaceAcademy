from fastapi import Body,FastAPI
from typing import Any, Dict, AnyStr, List, Union, Type
from pydantic import BaseModel
from google.cloud import pubsub_v1
import time
import json

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import gcsfs
import pandas_gbq
from datetime import datetime

import collections

project_id = "pelagic-campus-312012"
topic_name = "trialPubSub"
fileMyJsonCre = "D:/BlankSpace.io/Code BlankSpace.io/04_API/FASTAPI/pelagic-campus-312012-9bc9ceda9f9e.json"
credentialsMyGcp = service_account.Credentials.from_service_account_file(fileMyJsonCre)
client = bigquery.Client(credentials= credentialsMyGcp,project=project_id)

app = FastAPI()

# Run using uvicorn gatekeeper:app --reload

JSONObject = Dict[AnyStr, Any]
JSONArray = List[Any]
JSONStructure = List[Union[JSONArray, JSONObject]]

class Old_value(BaseModel):
    col_names: List[str]
    col_types: List[str]
    col_values: List[str] 

class Delete(BaseModel):
    operation: str
    table: str
    old_value: Old_value = Body(..., embed=True)

class Insert(BaseModel):
    operation: str
    table: str
    col_names: List[str]
    col_types: List[str]
    col_values: List[str]

JSONStructure = List[Union[Insert,Delete]]

@app.post("/post")
async def root(arbitrary_json: JSONStructure):
    global app_json
    thisdict =	{"activities":[]}

    for i in range (0,len(arbitrary_json)):
        if arbitrary_json[i].operation == "insert":
            thisdict["activities"].append({"operation":arbitrary_json[i].operation,
                                      "table":arbitrary_json[i].table,
                                      "col_names":arbitrary_json[i].col_names,
                                      "col_types":arbitrary_json[i].col_types,
                                      "col_values":arbitrary_json[i].col_values,
                                     })
        elif arbitrary_json[i].operation == "delete":
            thisdict["activities"].append({"operation":arbitrary_json[i].operation,
                                           "table":arbitrary_json[i].table,
                                           "old_value":{
                                                        "col_names":arbitrary_json[i].old_value.col_names,
                                                        "col_types":arbitrary_json[i].old_value.col_types,
                                                        "col_values":arbitrary_json[i].old_value.col_values,
                                                        }})

    app_json = json.dumps(thisdict)
    print(app_json)                                   

    pubsub_send()
    
    for i in range (0,len(arbitrary_json)):
        global operation, table 

        operation = arbitrary_json[i].operation
        table = arbitrary_json[i].table  

        print ("operation name :" + arbitrary_json[i].operation) 
        print ("table :" + arbitrary_json[i].table) 

        if arbitrary_json[i].operation == "insert":
            get_table()
            print ("Total table :" + str(total_table))

            if total_table == 1:

                get_insert_column()

                if collections.Counter(arbitrary_json[i].col_names) == collections.Counter(col_big_query):
                    print("insert with existing table")
                    col_name = ""
                    col_type =  "" 
                    col_values = ""

                    x = 0
                    for j in arbitrary_json[i].col_names:
                        if x == 0 :  
                            col_name = j
                        elif x!= 0 :
                            col_name = col_name + "," + j
                        x = x + 1
                    x = 0

                    for j in arbitrary_json[i].col_values:
                        if x == 0 :  
                            col_values = "'" + str(j) + "'"
                        elif x!= 0 :
                            col_values = col_values + ",'" + j + "'"
                        x = x + 1
                    x = 0

                    sql = "insert into Pubsub." + str(arbitrary_json[i].table) + "(" + str(col_name) + ") values (" + str(col_values) + ")"
                    print (sql)

                    query = client.query(sql)
                    results = query.result()

                else:

                    print("coloumn is not same")
 
            elif total_table == 0:
                ins_col = ""

                print("Adding table and insert data")

                if len(arbitrary_json[i].col_names) == len(arbitrary_json[i].col_values):
                    y = 0
                    for k in range (0, len(arbitrary_json[i].col_names)):
                        if y == 0:
                            ins_col = ins_col + arbitrary_json[i].col_names[k] + " " + arbitrary_json[i].col_types[k] 
                        elif y != 0:
                            ins_col = ins_col + ", " + arbitrary_json[i].col_names[k] + " " + arbitrary_json[i].col_types[k] 
                        y = y + 1

                    sql = "create table Pubsub." + str(arbitrary_json[i].table) + " (" + str(ins_col) + ")"
                    print (sql)

                    query = client.query(sql)
                    results = query.result()

                    col_name = ""
                    col_type =  "" 
                    col_values = ""

                    x = 0
                    for j in arbitrary_json[i].col_names:
                        if x == 0 :  
                            col_name = j
                        elif x!= 0 :
                            col_name = col_name + "," + j
                        x = x + 1
                    x = 0

                    for j in arbitrary_json[i].col_values:
                        if x == 0 :  
                            col_values = "'" + str(j) + "'"
                        elif x!= 0 :
                            col_values = col_values + ",'" + j + "'"
                        x = x + 1
                    x = 0

                    sql = "insert into Pubsub." + str(arbitrary_json[i].table) + "(" + str(col_name) + ") values (" + str(col_values) + ")"
                    print (sql)

                    query = client.query(sql)
                    results = query.result()

        elif arbitrary_json[i].operation == "delete":
            get_table()
            print ("Total table :" + str(total_table))

            if total_table == 1:
                del_col = ""
                print("delete with existing table")
                get_insert_column ()
                
                if collections.Counter(arbitrary_json[i].old_value.col_names) == collections.Counter(col_big_query):

                    if len(arbitrary_json[i].old_value.col_names) == len(arbitrary_json[i].old_value.col_values):
                        a = 0
                        for b in range (0, len(arbitrary_json[i].old_value.col_names)):
                            if a == 0:
                                del_col = del_col + arbitrary_json[i].old_value.col_names[b] + " ='" + arbitrary_json[i].old_value.col_values[b] + "'"
                            elif a != 0:
                                del_col = del_col + " and  " + arbitrary_json[i].old_value.col_names[b] + " ='" + arbitrary_json[i].old_value.col_values[b] + "'"
                            a = a + 1

                        sql = "delete from Pubsub." + str(arbitrary_json[i].table) + " where " + str(del_col)
                        print (sql)     

                        query = client.query(sql)
                        results = query.result()

            elif total_table != 1:
                print("cannot delete data")            

    return {"activities": arbitrary_json}

def pubsub_send():
    publisher = pubsub_v1.PublisherClient(credentials=credentialsMyGcp)
    topic_path = publisher.topic_path(project_id, topic_name)

    futures = dict()
    data = str(app_json)
    futures.update({data: None})
    future = publisher.publish(topic_path, data.encode("utf-8"))
    print(data)
    futures[data] = future
    print(f"Published messages with error handler to {topic_path}.")

def get_table():
    global total_table

    sql = "SELECT count (*) as count_table FROM Pubsub.INFORMATION_SCHEMA.TABLES where table_name ='" + str(table) + "'"
    query = client.query(sql)
    results = query.result()

    df = results.to_dataframe()
    total_table = df.count_table[0]

def get_insert_column():
    global col_big_query
    sql = "SELECT column_name FROM pelagic-campus-312012.Pubsub.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS WHERE table_name='"+ str(table) + "'"
    query = client.query(sql)
    results = query.result()

    df = results.to_dataframe()

    col_big_query = []
    for i in df.column_name:
        col_big_query.append(i)

    print(col_big_query)