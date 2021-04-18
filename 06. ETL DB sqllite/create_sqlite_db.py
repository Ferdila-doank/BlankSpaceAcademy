import csv
import pymysql.cursors
from datetime import datetime, timedelta
import pandas as pd
from unidecode import unidecode
import sqlalchemy

class MySQLCreate(object):
    def __init__(self, host, user, password):
        self.connection = pymysql.connect(host=host,
                                     user=user,
                                     password=password,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

    #def fill_db_temp_sqllite_csv(self,out_file,sqllite_source):
    def fill_db_temp_sqllite_csv(self,out_file,sqllite_source):
        self.create_table_temp_sqllite_csv()
        self.write_to_db_temp_sqllite_csv(sqllite_source)
        self.write_table_to_csv_temp_sqllite_csv(out_file)

    #Create the database and table temporary
    def create_table_temp_sqllite_csv(self):
        with self.connection.cursor() as cursor:
            cursor.execute("create database IF NOT EXISTS sqllite2;")
            cursor.execute("use sqllite2;") 

            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_artists (" \
                       "review_id int(6)," \
                       "artist text" \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_content (" \
                       "review_id int(6)," \
                       "content text " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_genres (" \
                       "review_id int(6)," \
                       "genre text " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_labels (" \
                       "review_id int(6)," \
                       "label text " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)           
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_reviews (" \
                       "review_id int(5)," \
                       "title text, " \
                       "artist text, " \
                       "url text, " \
                       "score text, " \
                       "best_new_music int(1), " \
                       "author text, " \
                       "author_type text, " \
                       "pub_date text, " \
                       "pub_weekday int(1), " \
                       "pub_day int(2), " \
                       "pub_month int(2), " \
                       "pub_year int(2) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_years (" \
                       "review_id int(6)," \
                       "year int(4) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)

        self.connection.commit()

    #extract csv convert to mysql
    def write_to_db_temp_sqllite_csv(self,sqllite_source):

        with self.connection.cursor() as cursor:
            db_name = sqllite_source
            table_name = ["artists","content","genres","labels","reviews","years"]

            for table_list in table_name:

                engine = sqlalchemy.create_engine("sqlite:///%s" % db_name, execution_options={"sqlite_raw_colnames": True})
                df = pd.read_sql_table(table_list, engine)
                
                if table_list == "artists":
                    cursor.execute("use sqllite2")
                    cursor.execute("delete from temp_artists")
                    data = list(zip(df['reviewid'],df['artist']))
                    insert_query = "insert into temp_artists (review_id,artist)" \
                                   "values (%s,%s)"
                    cursor.executemany(insert_query,data)
                    
                elif table_list == "content":
                    cursor.execute("use sqllite2")
                    cursor.execute("delete from temp_content")
                    data = list(zip(df['reviewid'],df['content']))
                    insert_query = "insert into temp_content (review_id,content)" \
                                   "values (%s,%s)"
                    cursor.executemany(insert_query,data)

                elif table_list == "genres":
                    cursor.execute("use sqllite2")
                    cursor.execute("delete from temp_genres")
                    data = list(zip(df['reviewid'],df['genre']))
                    insert_query = "insert into temp_genres (review_id,genre)" \
                                   "values (%s,%s)"                   
                    cursor.executemany(insert_query,data)

                elif table_list == "labels":
                    cursor.execute("use sqllite2")
                    cursor.execute("delete from temp_labels")
                    data = list(zip(df['reviewid'],df['label']))
                    insert_query = "insert into temp_labels (review_id,label) " \
                                   "values (%s,%s)"
                    cursor.executemany(insert_query,data)

                elif table_list == "reviews":
                    cursor.execute("use sqllite2")
                    cursor.execute("delete from temp_reviews")
                    data = list(zip(df['reviewid'],df['title'],df['artist'],df['url'],df['score'], \
                                    df['best_new_music'],df['author'],df['author_type'],df['pub_date'], \
                                    df['pub_weekday'],df['pub_day'],df['pub_month'],df['pub_year']))
                    insert_query = "insert into temp_reviews (review_id,title,artist,url,score," \
                                    "best_new_music,author,author_type,pub_date,pub_weekday,pub_day,pub_month,pub_year) " \
                                   "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.executemany(insert_query,data)

                elif table_list == "years":
                    cursor.execute("use sqllite2")
                    cursor.execute("delete from temp_years")
                    df['year'] = df['year'].fillna(0)
                    data = list(zip(df['reviewid'],df['year']))
                    insert_query = "insert into temp_years (review_id,year) " \
                                   "values (%s,%s)"
                    cursor.executemany(insert_query,data)

        self.connection.commit()

    def write_table_to_csv_temp_sqllite_csv(self, out_file):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT table_name as name FROM information_schema.tables WHERE table_schema = 'sqllite2';")
            rows = cursor.fetchall()

            with open(out_file, "w") as out:
                out.write("table_name")
                for row in rows:
                    out.write("\n")
                    table_name = row['name']
                    out.write('{}'.format(table_name))
        self.connection.commit()


# if __name__ == '__main__':
#     sqlCreate = MySQLCreate(host='localhost', user='root', password='boss')
#     sqlCreate.fill_db_temp_sqllite_csv('create-sql-db.csv','database.sqlite')

