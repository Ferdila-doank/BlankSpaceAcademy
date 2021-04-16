import luigi
from create_reviews_db import MySQLCreate
from datetime import datetime
from unidecode import unidecode

# Luigi Task to populate a MySQL table with the data given in the sample dateset
class CreateTableFromCsv(luigi.Task):
    rundate = datetime.now().strftime('%Y-%m-%d %H%M%S')
    host = 'localhost'
    user = 'root'
    password = 'boss'
    database = 'reviews'
    table = 'reviews'
    out_file = 'sql-db-{}.csv'.format(str(rundate))
    csv_source = 'reviews_q1.csv'

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(self.out_file)

    def run(self):
        mysql_db = MySQLCreate(host=self.host,
                               user=self.user,
                               password=self.password)
        mysql_db.fill_db_temp_reviews_csv(self.out_file,self.csv_source)


class ExportSqlDB(luigi.Task):
    rundate = datetime.now().strftime('%Y-%m-%d %H%M%S')
    host = 'localhost'
    user = 'root'
    password = 'boss'
    out_file = 'output-{}.csv'.format(str(rundate))

    def requires(self):
        return [CreateTableFromCsv()]

    def output(self):
        return luigi.LocalTarget(self.out_file)

    def run(self):
        mysql_db = MySQLCreate(host=self.host,
                               user=self.user,
                               password=self.password)
        db_connection = mysql_db.connection
        self.Temp_To_Reviews(db_connection)
        self.write_table_to_csv(db_connection, self.out_file)

    @staticmethod
    def Temp_To_Reviews(db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("use reviews")
            cursor.execute("CREATE TABLE IF NOT EXISTS reviews (listing_id text,id text,date datetime,reviewer_id text,reviewer_name text,comments longtext);")
            cursor.execute(
                "INSERT INTO reviews (listing_id,id,date,reviewer_id,reviewer_name,comments) "
                "SELECT * FROM temp_reviews "
                "WHERE id NOT IN "
                "(SELECT id FROM reviews);")
 
        db_connection.commit()

    @staticmethod
    def write_table_to_csv(db_connection, out_file):
        with db_connection.cursor() as cursor:
            cursor.execute("select * from reviews;")
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
        db_connection.commit()


if __name__ == '__main__':
    # Call on the last Task in the graph.
    luigi.run(["--local-scheduler"], main_task_cls=ExportSqlDB)
