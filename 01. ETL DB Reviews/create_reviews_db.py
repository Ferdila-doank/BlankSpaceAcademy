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

    def fill_db_temp_reviews_csv(self,out_file,csv_source):
        self.create_table_temp_reviews_csv()
        self.write_to_db_temp_reviews_csv(csv_source)
        self.write_table_to_csv_temp_reviews_csv(out_file)

    #Create the database and table temporary
    def create_table_temp_reviews_csv(self):
        with self.connection.cursor() as cursor:
            cursor.execute("create database IF NOT EXISTS reviews;")
            cursor.execute("use reviews;")
            cursor.execute("drop table if exists temp_reviews")
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_reviews (" \
                       "listing_id text," \
                       "id text, " \
                       "date datetime," \
                       "reviewer_id text, " \
                       "reviewer_name text, " \
                       "comments longtext " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)

        self.connection.commit()

    #extract csv convert to mysql
    def write_to_db_temp_reviews_csv(self,csv_source):
        with self.connection.cursor() as cursor:

            with open(csv_source, 'r', encoding="utf8") as raw_file:
                raw_data = csv.reader(raw_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                data_list = list(raw_data)[1:]
                
            for row in data_list: 
                listing_id = row[0]
                id = row[1]
                date = datetime.strptime(row[2], '%Y-%m-%d')
                reviewer_id = row[3]                    
                reviewer_name = row[4]
                comments = row[5]
                comments = comments.replace('"','')
                sql_query = 'INSERT INTO temp_reviews (listing_id,id,date,reviewer_id,reviewer_name,comments) ' \
                            'values ("{}", "{}", "{}", "{}", "{}", "{}");'.format(listing_id, id, date, reviewer_id,reviewer_name,comments) 
                cursor.execute(sql_query)
        self.connection.commit()

    # Mysql to csv
    def write_table_to_csv_temp_reviews_csv(self, out_file):
        with self.connection.cursor() as cursor:
            #listing_id,id,date,reviewer_id,reviewer_name,comments
            cursor.execute("select * from temp_reviews;")
            rows = cursor.fetchall()

            with open(out_file, "w") as out:
                out.write("listing_id,id,date,reviewer_id,reviewer_name,comments")
                for row in rows:
                    out.write("\n")
                    listing_id = row['listing_id']
                    id = row['id']
                    date = row['date'].strftime('%Y-%m-%d')
                    reviewer_id = row['reviewer_id']
                    reviewer_name = unidecode(row['reviewer_name'])
                    comments = unidecode(row['comments'])
                    out.write('{}, {}, {}, {}, {}, {}'.format(listing_id, id, date, reviewer_id,reviewer_name,comments))
        self.connection.commit()


#if __name__ == '__main__':
#    sqlCreate = MySQLCreate(host='localhost', user='root', password='boss')
#    sqlCreate.fill_db_temp_reviews_csv('create-sql-db.csv','reviews_q1.csv')

