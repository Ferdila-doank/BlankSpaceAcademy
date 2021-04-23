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
    database = 'sqllite2'
    out_file = 'sql-db-{}.csv'.format(str(rundate))
    sqllite_source = 'database.sqlite'

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
    database = 'sqllite2'
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
            cursor.execute("use sqllite2")

            sqlQuery = "CREATE TABLE IF NOT EXISTS artists (" \
                       "review_id int(6)," \
                       "artist text" \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)

            cursor.execute(
                "INSERT INTO artists (review_id,artist)" 
                "SELECT * FROM temp_artists "
                "WHERE review_id NOT IN "
                "(SELECT review_id FROM artists);")

            sqlQuery = "CREATE TABLE IF NOT EXISTS content (" \
                       "review_id int(6)," \
                       "content text " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)

            cursor.execute(
                "INSERT INTO content (review_id,content)" 
                "SELECT * FROM temp_content "
                "WHERE review_id NOT IN "
                "(SELECT review_id FROM content);")

            sqlQuery = "CREATE TABLE IF NOT EXISTS genres (" \
                       "review_id int(6)," \
                       "genre text " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)

            cursor.execute(
                "INSERT INTO genres (review_id,genre)" 
                "SELECT * FROM temp_genres "
                "WHERE review_id NOT IN "
                "(SELECT review_id FROM genres);")

            sqlQuery = "CREATE TABLE IF NOT EXISTS labels (" \
                       "review_id int(6)," \
                       "label text " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)

            cursor.execute(
                "INSERT INTO labels (review_id,label)" 
                "SELECT * FROM temp_labels "
                "WHERE review_id NOT IN "
                "(SELECT review_id FROM labels);")

            sqlQuery = "CREATE TABLE IF NOT EXISTS reviews (" \
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

            cursor.execute(
                "INSERT INTO reviews (review_id,title,artist,url,score,best_new_music,author,author_type,pub_date,pub_weekday,pub_day,pub_month,pub_year)" 
                "SELECT * FROM temp_reviews "
                "WHERE review_id NOT IN "
                "(SELECT review_id FROM reviews);")

            sqlQuery = "CREATE TABLE IF NOT EXISTS years (" \
                       "review_id int(6)," \
                       "year int(4) " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)

            cursor.execute(
                "INSERT INTO years (review_id,year)" 
                "SELECT * FROM temp_years "
                "WHERE review_id NOT IN "
                "(SELECT review_id FROM years);")

        db_connection.commit()

    @staticmethod
    def write_table_to_csv(db_connection, out_file):
        with db_connection.cursor() as cursor:
            cursor.execute("use sqllite2")
            cursor.execute("select * from artists;")
            rows = cursor.fetchall()
            with open(out_file + ("_artists.csv") , "w",encoding="utf-8") as out:
                out.write("review_id,artist")
                for row in rows:
                    out.write("\n")
                    review_id = row['review_id']
                    artist = row['artist']
                    out.write('{}, {}'.format(review_id,artist))

            cursor.execute("select * from content;")
            rows = cursor.fetchall()     
            with open(out_file + ("_content.csv") , "w",encoding="utf-8") as out:
                out.write("review_id,content")
                for row in rows:
                    out.write("\n")
                    review_id = row['review_id']
                    content = row['content']
                    out.write('{}, {}'.format(review_id,content))

            cursor.execute("select * from genres;")
            rows = cursor.fetchall()     
            with open(out_file + ("_genres.csv") , "w",encoding="utf-8") as out:
                out.write("review_id,genre")
                for row in rows:
                    out.write("\n")
                    review_id = row['review_id']
                    genre = row['genre']
                    out.write('{}, {}'.format(review_id,genre))

            cursor.execute("select * from labels;")
            rows = cursor.fetchall()     
            with open(out_file + ("_labels.csv") , "w",encoding="utf-8") as out:
                out.write("review_id,label")
                for row in rows:
                    out.write("\n")
                    review_id = row['review_id']
                    label = row['label']
                    out.write('{}, {}'.format(review_id,label))

            cursor.execute("select * from reviews;")
            rows = cursor.fetchall()     
            with open(out_file + ("_reviews.csv") , "w", encoding="utf-8") as out:
                out.write("review_id,artist,url,score,best_new_music,author,author_type,pub_date,pub_weekday,pub_day,pub_month,pub_year")
                for row in rows:
                    out.write("\n")
                    review_id = row['review_id']
                    artist = row['artist']
                    url = row['url']
                    score = row['score']
                    best_new_music = row['best_new_music']
                    author = row['author']
                    author_type = row['author_type']
                    pub_date = row['pub_date']
                    pub_weekday = row['pub_weekday']
                    pub_day = row['pub_day']
                    pub_month = row['pub_month']
                    pub_year = row['pub_year']
                    out.write('{},{},{},{},{}, {},{}, {},{}, {},{}, {}'.format(review_id,artist,url,score,best_new_music,author,author_type,pub_date,pub_weekday,pub_day,pub_month,pub_year))                   

            cursor.execute("select * from years;")
            rows = cursor.fetchall()
            with open(out_file + ("_years.csv") , "w",encoding="utf-8") as out:
                out.write("review_id,year")
                for row in rows:
                    out.write("\n")
                    review_id = row['review_id']
                    year = row['year']
                    out.write('{}, {}'.format(review_id,year))
                    
        db_connection.commit()


if __name__ == '__main__':
    # Call on the last Task in the graph.
    luigi.run(["--local-scheduler"], main_task_cls=ExportSqlDB)
