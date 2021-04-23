import csv
import pymysql.cursors
from datetime import datetime, timedelta
import pandas as pd
from unidecode import unidecode

class MySQLCreate(object):
    def __init__(self, host, user, password):
        self.connection = pymysql.connect(host=host,
                                     user=user,
                                     password=password,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

    def fill_db_temp_disaster_csv(self,out_file,csv_source):
        self.create_table_temp_disaster_csv()
        self.write_to_db_temp_disaster_csv(csv_source)
        self.write_table_to_csv_temp_disaster_csv(out_file)

    #Create the database and table temporary
    def create_table_temp_disaster_csv(self):
        with self.connection.cursor() as cursor:
            cursor.execute("create database IF NOT EXISTS disaster;")
            cursor.execute("use disaster;")
            cursor.execute("drop table if exists temp_disaster")
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_disaster (" \
                       "id text," \
                       "keyword_disaster text," \
                       "location_disaster text, " \
                       "textcomments longtext," \
                       "target text " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)

        self.connection.commit()

    #extract csv convert to mysql
    def write_to_db_temp_disaster_csv(self,csv_source):
        with self.connection.cursor() as cursor:

            with open(csv_source, 'r', encoding="utf8") as raw_file:
                raw_data = csv.reader(raw_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                data_list = list(raw_data)[1:]

            for row in data_list:
                try:
                    #id,keyword,location,text,target
                    id = row[0]
                    keyword_disaster = row[1]
                    keyword_disaster = keyword_disaster.replace("%20"," ")
                    location_disaster = row[2]
                    textcomments = row[3]
                    if textcomments != None:
                        string_encode = unidecode(textcomments).encode("ascii", "ignore")
                        string_decode = string_encode.decode()     
                    target = row[4] 
                    sql_query = 'INSERT INTO temp_disaster (id,keyword_disaster,location_disaster,textcomments,target) ' \
                                'values ("{}","{}","{}","{}","{}");'.format(id,keyword_disaster,location_disaster,string_decode,target) 
                    cursor.execute(sql_query)
                except:
                    pass
        self.connection.commit()

    # Mysql to csv
    def write_table_to_csv_temp_disaster_csv(self, out_file):
        with self.connection.cursor() as cursor:
            #listing_id,id,date,reviewer_id,reviewer_name,comments
            cursor.execute("select id,keyword_disaster,location_disaster,textcomments,target from temp_disaster;")
            rows = cursor.fetchall()

            with open(out_file, "w") as out:
                out.write("id,keyword,location,textcomments,target")
                for row in rows:
                    out.write("\n")
                    id = row['id']
                    keyword_disaster = row['keyword_disaster']
                    location_disaster = unidecode(row['location_disaster'])
                    textcomments = row['textcomments']
                    target = row['target']
                    out.write('{},{},{},{},{}'.format(id,keyword_disaster,location_disaster,textcomments,target))
        self.connection.commit()


if __name__ == '__main__':
    sqlCreate = MySQLCreate(host='localhost', user='root', password='boss')
    sqlCreate.fill_db_temp_disaster_csv('create-sql-db.csv','disaster_data.csv')

