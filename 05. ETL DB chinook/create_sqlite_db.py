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
            cursor.execute("create database IF NOT EXISTS sqllite;")
            cursor.execute("use sqllite;")       
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_albums (" \
                       "album_id int(5)," \
                       "Last_Name varchar(160), " \
                       "artist_id int(5)" \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_artists (" \
                       "artist_id int(5)," \
                       "name varchar(120) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)           
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_customers (" \
                       "customer_id int(5)," \
                       "first_name varchar(40), " \
                       "last_name varchar(20), " \
                       "company varchar(80), " \
                       "address varchar(70), " \
                       "city varchar(40), " \
                       "State varchar(40), " \
                       "country varchar(40), " \
                       "postal_code varchar(10), " \
                       "phone varchar(24), " \
                       "fax varchar(24), " \
                       "email varchar(60), " \
                       "support_rep_id int(5)" \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_employees (" \
                       "employee_id int(5)," \
                       "first_name varchar(20), " \
                       "last_name varchar(20), " \
                       "title varchar(30), " \
                       "reports_to int(70), " \
                       "birth_date datetime, " \
                       "hire_date datetime, " \
                       "address varchar(70)," \
                       "city varchar(40)," \
                       "state varchar(40)," \
                       "country varchar(40), " \
                       "postal_code varchar(10), " \
                       "phone varchar(24), " \
                       "fax varchar(24), " \
                       "email varchar(60) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_genres (" \
                       "genre_id int(5)," \
                       "name varchar(120) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_invoice_items (" \
                       "invoice_line_id int(5)," \
                       "invoice_id int(5), " \
                       "track_id int(5), " \
                       "unit_price float(10,2), " \
                       "quantity int(5) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_invoice (" \
                       "invoice_id int(5)," \
                       "customer_id int(5), " \
                       "invoice_date datetime, " \
                       "billing_address varchar(70), " \
                       "billing_city varchar(40), " \
                       "billing_state varchar(40), " \
                       "billing_country varchar(40), " \
                       "billing_postal_code varchar(10), " \
                       "total float(10,2) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_media_types (" \
                       "media_types_id int(5)," \
                       "name varchar(120) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_playlist_track (" \
                       "playlist_id int(5)," \
                       "track_id int(5) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery) 
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_playlist (" \
                       "playlist_id int(5)," \
                       "name varchar(120) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_tracks (" \
                       "tracks_id int(4)," \
                       "name varchar(200), " \
                       "album_id int(5), " \
                       "media_type_id int(5), " \
                       "genre_id int(5), " \
                       "composer varchar(220), " \
                       "miliseconds int(10), " \
                       "bytes int(5), " \
                       "unit_price float(10,2) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)            
        self.connection.commit()

    #extract csv convert to mysql
    def write_to_db_temp_sqllite_csv(self,sqllite_source):

        with self.connection.cursor() as cursor:
            db_name = sqllite_source
            table_name = ["albums","artists","customers","employees","genres","invoice_items","invoices","media_types","playlist_track","playlists","tracks"]

            for table_list in table_name:

                engine = sqlalchemy.create_engine("sqlite:///%s" % db_name, execution_options={"sqlite_raw_colnames": True})
                df = pd.read_sql_table(table_list, engine)
                
                if table_list == "albums":
                    cursor.execute("use sqllite")
                    cursor.execute("delete from temp_albums")
                    data = list(zip(df['AlbumId'],df['Title'],df['ArtistId']))
                    insert_query = "insert into temp_albums (album_id,Last_Name,artist_id)" \
                                   "values (%s,%s,%s)"
                    cursor.executemany(insert_query,data)
                    
                elif table_list == "artists":
                    cursor.execute("use sqllite")
                    cursor.execute("delete from temp_artists")
                    data = list(zip(df['ArtistId'],df['Name']))
                    insert_query = "insert into temp_artists (artist_id,name)" \
                                   "values (%s,%s)"
                    cursor.executemany(insert_query,data)

                elif table_list == "customers":
                    cursor.execute("use sqllite")
                    cursor.execute("delete from temp_customers")
                    data = list(zip(df['CustomerId'],df['FirstName'],df['LastName'],df['Company'],df['Address'], \
                                    df['City'],df['State'],df['Country'],df['PostalCode'],df['Phone'],df['Fax'], \
                                    df['Email'],df['SupportRepId']))
                    insert_query = "insert into temp_customers (customer_id,first_name,last_name,company,address," \
                                   "city,State,country,postal_code,phone,fax,email,support_rep_id)" \
                                   "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.executemany(insert_query,data)

                elif table_list == "genres":
                    cursor.execute("use sqllite")
                    cursor.execute("delete from temp_genres")
                    data = list(zip(df['GenreId'],df['Name']))
                    insert_query = "insert into temp_genres (genre_id,name)" \
                                   "values (%s,%s)"                   
                    cursor.executemany(insert_query,data)

                elif table_list == "invoice_items":
                    cursor.execute("use sqllite")
                    cursor.execute("delete from temp_invoice_items")
                    data = list(zip(df['InvoiceLineId'],df['InvoiceId'],df['TrackId'],df['UnitPrice'],df['Quantity']))
                    insert_query = "insert into temp_invoice_items (invoice_line_id,invoice_id,track_id,unit_price,quantity) " \
                                   "values (%s,%s,%s,%s,%s)"
                    cursor.executemany(insert_query,data)

                elif table_list == "invoices":
                    cursor.execute("use sqllite")
                    cursor.execute("delete from temp_invoice")
                    data = list(zip(df['InvoiceId'],df['CustomerId'],df['InvoiceDate'],df['BillingAddress'],df['BillingCity'], \
                                    df['BillingState'],df['BillingCountry'],df['BillingPostalCode'],df['Total'] \
                                    ))
                    insert_query = "insert into temp_invoice (invoice_id,customer_id,invoice_date,billing_address,billing_city," \
                                    "billing_state,billing_country,billing_postal_code,total) " \
                                   "values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.executemany(insert_query,data)

                elif table_list == "media_types":
                    cursor.execute("use sqllite")
                    cursor.execute("delete from temp_media_types")
                    data = list(zip(df['MediaTypeId'],df['Name']))
                    insert_query = "insert into temp_media_types (media_types_id,name) " \
                                   "values (%s,%s)"
                    cursor.executemany(insert_query,data)

                elif table_list == "playlist_track":
                    cursor.execute("use sqllite")
                    cursor.execute("delete from temp_playlist_track")
                    data = list(zip(df['PlaylistId'],df['TrackId']))
                    insert_query = "insert into temp_playlist_track (playlist_id,track_id) " \
                                   "values (%s,%s)"
                    cursor.executemany(insert_query,data)

                elif table_list == "playlists":
                    cursor.execute("use sqllite")
                    cursor.execute("delete from temp_playlist")
                    data = list(zip(df['PlaylistId'],df['Name']))
                    insert_query = "insert into temp_playlist (playlist_id,name) " \
                                   "values (%s,%s)"
                    cursor.executemany(insert_query,data)

                elif table_list == "tracks":
                    cursor.execute("use sqllite")
                    cursor.execute("delete from temp_tracks")
                    data = list(zip(df['TrackId'],df['Name'],df['AlbumId'],df['MediaTypeId'], \
                                    df['GenreId'],df['Composer'],df['Milliseconds'],df['Bytes'],df['UnitPrice'])) 
                    insert_query = "insert into temp_tracks (tracks_id,name,album_id,media_type_id,genre_id," \
                                   "composer,miliseconds,bytes,unit_price) " \
                                   "values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.executemany(insert_query,data)

        self.connection.commit()

    def write_table_to_csv_temp_sqllite_csv(self, out_file):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT table_name as name FROM information_schema.tables WHERE table_schema = 'sqllite';")
            rows = cursor.fetchall()

            with open(out_file, "w") as out:
                out.write("table_name")
                for row in rows:
                    out.write("\n")
                    table_name = row['name']
                    out.write('{}'.format(table_name))
        self.connection.commit()


if __name__ == '__main__':
    sqlCreate = MySQLCreate(host='localhost', user='root', password='boss')
    sqlCreate.fill_db_temp_sqllite_csv('create-sql-db.csv','chinook.db')

