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

    def fill_db_temp_xls_csv(self,out_file,xls_source):
        self.create_table_temp_xls_csv()
        self.write_to_db_temp_xls_csv(xls_source)
        self.write_table_to_csv_temp_xls_csv(out_file)

    #Create the database and table temporary
    def create_table_temp_xls_csv(self):
        with self.connection.cursor() as cursor:
            cursor.execute("create database IF NOT EXISTS xls;")
            cursor.execute("use xls;")
            cursor.execute("drop table if exists temp_xls")
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_xls (" \
                       "First_Name text," \
                       "Last_Name text, " \
                       "Gender text," \
                       "Country text, " \
                       "Age_Date date, " \
                       "id text " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)

        self.connection.commit()

    #extract csv convert to mysql
    def write_to_db_temp_xls_csv(self,xls_source):
        with self.connection.cursor() as cursor:
            df = pd.read_excel(xls_source, sheet_name='Sheet2')
            if 'Unnamed: 0' in df.columns:
                df.drop(['Unnamed: 0'],axis = 1, inplace = True)
            df.to_csv('xls.csv', index=False)
            with open('xls.csv', 'r', encoding="utf8") as raw_file:
                raw_data = csv.reader(raw_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                data_list = list(raw_data)[1:]
          
            for row in data_list: 
                First_Name = row[0]
                Last_Name = row[1]
                Gender = row[2]
                Country = row[3]
                id = row[4]
                Age_Date = datetime.strptime(row[5], '%d/%m/%Y')
                sql_query = 'INSERT INTO temp_xls (First_Name,Last_Name,Gender,Country,Age_Date,id) ' \
                            'values ("{}", "{}", "{}", "{}", "{}", "{}");'.format(First_Name,Last_Name,Gender,Country,Age_Date,id) 
                cursor.execute(sql_query)
        self.connection.commit()

    # Mysql to csv
    def write_table_to_csv_temp_xls_csv(self, out_file):
        with self.connection.cursor() as cursor:
            #First_Name,Last_Name,Gender,Country,Age_Date,id
            cursor.execute("select * from temp_xls;")
            rows = cursor.fetchall()

            with open(out_file, "w") as out:
                out.write("First_Name,Last_Name,Gender,Country,Age_Date,id")
                for row in rows:
                    out.write("\n")
                    First_Name = row['First_Name']
                    Last_Name = row['Last_Name']
                    Gender = row['Gender']
                    Country = row['Country']
                    Age_Date = row['Age_Date']
                    id = row['id']
                    out.write('{}, {}, {}, {}, {}, {}'.format(First_Name,Last_Name,Gender,Country,Age_Date,id))
        self.connection.commit()


#if __name__ == '__main__':
#    sqlCreate = MySQLCreate(host='localhost', user='root', password='boss')
#    sqlCreate.fill_db_temp_xls_csv('create-sql-db.csv','file_1000.xls')

