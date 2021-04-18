import luigi
from create_sqlite_db import MySQLCreate
from datetime import datetime
from unidecode import unidecode

# Luigi Task to populate a MySQL table with the data given in the sample dateset
class CreateTableFromCsv(luigi.Task):
    rundate = datetime.now().strftime('%Y-%m-%d %H%M%S')
    host = 'localhost'
    user = 'root'
    password = 'boss'
    database = 'sqllite'
    #table = 'albums'
    out_file = 'sql-db-{}.csv'.format(str(rundate))
    sqllite_source = 'chinook.db'

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(self.out_file)

    def run(self):
        mysql_db = MySQLCreate(host=self.host,
                               user=self.user,
                               password=self.password)
        mysql_db.fill_db_temp_sqllite_csv(self.out_file,self.sqllite_source)

class ExportSqlDB(luigi.Task):
    rundate = datetime.now().strftime('%Y-%m-%d %H%M%S')
    host = 'localhost'
    user = 'root'
    password = 'boss'
    database = 'sqllite'
    out_file = 'output-{}'.format(str(rundate))

    def requires(self):
        return [CreateTableFromCsv()]

    def output(self):
        return luigi.LocalTarget(self.out_file)

    def run(self):
        mysql_db = MySQLCreate(host=self.host,
                               user=self.user,
                               password=self.password)
        db_connection = mysql_db.connection
        self.Temp_To_xls(db_connection)
        self.write_table_to_csv(db_connection, self.out_file)

    @staticmethod
    def Temp_To_xls(db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("use sqllite")
            cursor.execute("CREATE TABLE IF NOT EXISTS albums (album_id int(5),Last_Name varchar(160),artist_id int(5));")

            cursor.execute(
                "INSERT INTO albums (album_id,Last_Name,artist_id)" 
                "SELECT * FROM temp_albums "
                "WHERE album_id NOT IN "
                "(SELECT album_id FROM albums);")
            
            sqlQuery = "CREATE TABLE IF NOT EXISTS artists (" \
                       "artist_id int(5)," \
                       "name varchar(120) " \
                       ");"
            cursor.execute(sqlQuery)

            cursor.execute(
                "INSERT INTO artists (artist_id,name)"
                "SELECT * FROM temp_artists "
                "WHERE artist_id NOT IN "
                "(SELECT artist_id FROM artists);") 

            sqlQuery = "CREATE TABLE IF NOT EXISTS customers (" \
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
                       ");"
            cursor.execute(sqlQuery)

            cursor.execute(
                 "INSERT INTO customers(customer_id,first_name,last_name,company,address,city,State,country,postal_code,phone,fax,email,support_rep_id)"
                 "SELECT * FROM temp_customers "
                 "WHERE customer_id NOT IN "
                 "(SELECT customer_id FROM customers);") 

            sqlQuery = "CREATE TABLE IF NOT EXISTS employees (" \
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
                       ");"
            cursor.execute(sqlQuery)

            cursor.execute(
                "INSERT INTO employees (employee_id,first_name,last_name,title,reports_to,birth_date,hire_date,address,city,state,country,postal_code,phone,fax,email)"
                "SELECT * FROM temp_employees "
                "WHERE employee_id NOT IN "
                "(SELECT employee_id FROM employees);") 

            sqlQuery = "CREATE TABLE IF NOT EXISTS genres (" \
                       "genre_id int(5)," \
                       "name varchar(120) " \
                       ");"
            cursor.execute(sqlQuery)

            cursor.execute(
                "INSERT INTO genres(genre_id,name)"
                "SELECT * FROM temp_genres "
                "WHERE genre_id NOT IN "
                "(SELECT genre_id FROM genres);") 

            sqlQuery = "CREATE TABLE IF NOT EXISTS invoice_items (" \
                       "invoice_line_id int(5)," \
                       "invoice_id int(5), " \
                       "track_id int(5), " \
                       "unit_price float(10,2), " \
                       "quantity int(5) " \
                       ");"
            cursor.execute(sqlQuery)

            cursor.execute(
                "INSERT INTO invoice_items(invoice_line_id,invoice_id,track_id,unit_price,quantity)"
                "SELECT * FROM temp_invoice_items "
                "WHERE invoice_line_id NOT IN "
                "(SELECT invoice_line_id FROM invoice_items);")

            sqlQuery = "CREATE TABLE IF NOT EXISTS invoice (" \
                       "invoice_id int(5)," \
                       "customer_id int(5), " \
                       "invoice_date datetime, " \
                       "billing_address varchar(70), " \
                       "billing_city varchar(40), " \
                       "billing_state varchar(40), " \
                       "billing_country varchar(40), " \
                       "billing_postal_code varchar(10), " \
                       "total float(10,2) " \
                       ");"
            cursor.execute(sqlQuery)

            cursor.execute(
                "INSERT INTO invoice(invoice_id,customer_id,invoice_date,billing_address,billing_city,billing_state,billing_country,billing_postal_code,total)"
                "SELECT * FROM temp_invoice "
                "WHERE invoice_id NOT IN "
                "(SELECT invoice_id FROM invoice);")

            sqlQuery = "CREATE TABLE IF NOT EXISTS media_types (" \
                       "media_types_id int(5)," \
                       "name varchar(120) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)

            cursor.execute(
                "INSERT INTO media_types(media_types_id,name)"
                "SELECT * FROM temp_media_types "
                "WHERE media_types_id NOT IN "
                "(SELECT media_types_id FROM media_types);")

            sqlQuery = "CREATE TABLE IF NOT EXISTS playlist_track (" \
                       "playlist_id int(5)," \
                       "track_id int(5) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery) 

            cursor.execute(
                "INSERT INTO playlist_track(playlist_id,track_id)"
                "SELECT * FROM temp_playlist_track "
                "WHERE playlist_id NOT IN "
                "(SELECT playlist_id FROM playlist_track);")

            sqlQuery = "CREATE TABLE IF NOT EXISTS playlist (" \
                       "playlist_id int(5)," \
                       "name varchar(120) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)

            cursor.execute(
                "INSERT INTO playlist(playlist_id,name)"
                "SELECT * FROM temp_playlist "
                "WHERE playlist_id NOT IN "
                "(SELECT playlist_id FROM playlist);")

            sqlQuery = "CREATE TABLE IF NOT EXISTS tracks (" \
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

            cursor.execute(
                "INSERT INTO tracks(tracks_id,name,album_id,media_type_id,genre_id,composer,miliseconds,bytes,unit_price)"
                "SELECT * FROM temp_tracks "
                "WHERE tracks_id NOT IN "
                "(SELECT tracks_id FROM tracks);")

        db_connection.commit()

    @staticmethod
    def write_table_to_csv(db_connection, out_file):
        with db_connection.cursor() as cursor:
            cursor.execute("use sqllite")
            cursor.execute("select * from albums;")
            rows = cursor.fetchall()
            with open(out_file + ("_albums.csv") , "w") as out:
                out.write("album_id,Last_Name,artist_id")
                for row in rows:
                    out.write("\n")
                    album_id = row['album_id']
                    Last_Name = row['Last_Name']
                    artist_id = row['artist_id']
                    out.write('{}, {}, {}'.format(album_id,Last_Name,artist_id))

                out.write("end table albums")
            cursor.execute("select * from artists;")
            rows = cursor.fetchall()     
            with open(out_file + ("_artists.csv") , "w") as out:
                out.write("artist_id,name")
                for row in rows:
                    out.write("\n")
                    artist_id = row['artist_id']
                    name = row['name']
                    out.write('{}, {}'.format(artist_id,name))

            cursor.execute("select * from customers;")
            rows = cursor.fetchall()     
            with open(out_file + ("_customers.csv") , "w", encoding="utf-8") as out:
                out.write("customer_id,first_name,last_name,company,address,city,State,country,postal_code,phone,fax,email,support_rep_id")
                for row in rows:
                    out.write("\n")
                    customer_id = row['customer_id']
                    first_name = row['first_name']
                    last_name = row['last_name']
                    company = row['company']
                    address = row['address']
                    city = row['city']
                    State = row['State']
                    country = row['country']
                    postal_code = row['postal_code']
                    phone = row['phone']
                    fax = row['fax']
                    email = row['email']
                    support_rep_id = row['support_rep_id']
                    out.write('{},{},{},{},{}, {},{}, {},{}, {},{}, {},{}'.format(customer_id,first_name,last_name,company,address,city,State,country,postal_code,phone,fax,email,support_rep_id))                   

            cursor.execute("select * from employees;")
            rows = cursor.fetchall()     
            with open(out_file + ("_employees.csv") , "w", encoding="utf-8") as out:
                out.write("employee_id,first_name,last_name,title,reports_to,birth_date,hire_date,address,city,state,country,postal_code,phone,fax,email")
                for row in rows:
                    out.write("\n")
                    employee_id = row['employee_id']
                    first_name = row['first_name']
                    last_name = row['last_name']
                    title = row['title']
                    reports_to = row['reports_to']
                    birth_date = row['birth_date']
                    hire_date = row['hire_date']
                    address = row['address']
                    city = row['city']
                    state = row['state']
                    country = row['country']
                    postal_code = row['postal_code']
                    phone = row['phone']
                    fax = row['fax']
                    email = row['email']
                    out.write('{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}' \
                              .format(employee_id,first_name,last_name,title,reports_to,birth_date,hire_date,address,city,state,country,postal_code,phone,fax,email))                  
            
            cursor.execute("select * from genres;")
            rows = cursor.fetchall()
            with open(out_file + ("_genres.csv") , "w") as out:
                out.write("genre_id,name")
                for row in rows:
                    out.write("\n")
                    genre_id = row['genre_id']
                    name = row['name']
                    out.write('{}, {}'.format(genre_id,name))

            cursor.execute("select * from invoice_items;")
            rows = cursor.fetchall()
            with open(out_file + ("_invoice_items.csv") , "w") as out:
                out.write("invoice_line_id,invoice_id,track_id,unit_price,quantity")
                for row in rows:
                    out.write("\n")
                    invoice_line_id = row['invoice_line_id']
                    invoice_id = row['invoice_id']
                    track_id = row['track_id']
                    unit_price = row['unit_price']
                    quantity = row['quantity']
                    out.write('{},{},{},{},{}'.format(invoice_line_id,invoice_id,track_id,unit_price,quantity)) 

            cursor.execute("select * from invoice;")
            rows = cursor.fetchall()
            with open(out_file + ("_invoice.csv") , "w") as out:
                out.write("invoice_id,customer_id,invoice_date,billing_address,billing_city,billing_state,billing_country,billing_postal_code,total")
                for row in rows:
                    out.write("\n")
                    invoice_id = row['invoice_id']
                    customer_id = row['customer_id']
                    invoice_date = row['invoice_date']
                    billing_address = row['billing_address']
                    billing_city = row['billing_city']
                    billing_state = row['billing_state']
                    billing_country = row['billing_country']
                    billing_postal_code = row['billing_postal_code']
                    total = row['total']
                    out.write('{},{},{},{},{},{},{},{},{}'.format(invoice_id,customer_id,invoice_date,billing_address,billing_city,billing_state,billing_country,billing_postal_code,total)) 

            cursor.execute("select * from media_types;")
            rows = cursor.fetchall()
            with open(out_file + ("_media_types.csv") , "w") as out:
                out.write("media_types_id,name")
                for row in rows:
                    out.write("\n")
                    media_types_id = row['media_types_id']
                    name = row['name']
                    out.write('{}, {}'.format(media_types_id,name))

            cursor.execute("select * from playlist_track;")
            rows = cursor.fetchall()
            with open(out_file + ("_playlist_track.csv") , "w") as out:
                out.write("playlist_id,track_id")
                for row in rows:
                    out.write("\n")
                    playlist_id = row['playlist_id']
                    track_id = row['track_id']
                    out.write('{}, {}'.format(playlist_id,track_id))

            cursor.execute("select * from playlist;")
            rows = cursor.fetchall()
            with open(out_file + ("_playlist.csv") , "w") as out:
                out.write("playlist_id,name")
                for row in rows:
                    out.write("\n")
                    playlist_id = row['playlist_id']
                    track_id = row['name']
                    out.write('{}, {}'.format(playlist_id,name))

            cursor.execute("select * from tracks;")
            rows = cursor.fetchall()
            with open(out_file + ("_tracks.csv") , "w") as out:
                out.write("tracks_id,name,album_id,media_type_id,genre_id,composer,miliseconds,bytes,unit_price")
                for row in rows:
                    out.write("\n")
                    tracks_id = row['tracks_id']
                    name = row['name']
                    album_id = row['album_id']
                    media_type_id = row['media_type_id']
                    genre_id = row['genre_id']
                    composer = row['composer']
                    miliseconds = row['miliseconds']
                    bytes = row['bytes']
                    unit_price = row['unit_price']
                    out.write('{},{},{},{},{},{},{},{},{}'.format(tracks_id,name,album_id,media_type_id,genre_id,composer,miliseconds,bytes,unit_price)) 

        db_connection.commit()


if __name__ == '__main__':
    # Call on the last Task in the graph.
    luigi.run(["--local-scheduler"], main_task_cls=ExportSqlDB)
