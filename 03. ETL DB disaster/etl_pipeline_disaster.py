import luigi
from create_disaster_db import MySQLCreate
from datetime import datetime
from unidecode import unidecode

# Luigi Task to populate a MySQL table with the data given in the sample dateset
class CreateTableFromCsv(luigi.Task):
    rundate = datetime.now().strftime('%Y-%m-%d %H%M%S')
    host = 'localhost'
    user = 'root'
    password = 'boss'
    database = 'disaster'
    table = 'disaster'
    out_file = 'sql-db-{}.csv'.format(str(rundate))
    csv_source = 'disaster_data.csv'

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(self.out_file)

    def run(self):
        mysql_db = MySQLCreate(host=self.host,
                               user=self.user,
                               password=self.password)
        mysql_db.fill_db_temp_disaster_csv(self.out_file,self.csv_source)


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
        self.Temp_To_Disaster(db_connection)
        self.write_table_to_csv(db_connection, self.out_file)

    @staticmethod
    def Temp_To_Disaster(db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("use disaster")
            cursor.execute("CREATE TABLE IF NOT EXISTS disaster (id text,keyword_disaster text,location_disaster text,textcomments longtext,target text);")
            cursor.execute(
                "INSERT INTO disaster (id,keyword_disaster,location_disaster,textcomments,target) "
                "SELECT * FROM temp_disaster "
                "WHERE id NOT IN "
                "(SELECT id FROM disaster);")
 
        db_connection.commit()

    @staticmethod
    def write_table_to_csv(db_connection, out_file):
        with db_connection.cursor() as cursor:
            cursor.execute("select * from disaster;")
            rows = cursor.fetchall()
            with open(out_file, "w") as out:
                out.write("id,keyword_disaster,location_disaster,textcomments,target")
                for row in rows:
                    out.write("\n")
                    id = row['id']
                    keyword_disaster = row['keyword_disaster']
                    location_disaster = unidecode(row['location_disaster'])
                    textcomments = row['textcomments']
                    target = row['target']
                    out.write('{},{},{},{},{}'.format(id,keyword_disaster,location_disaster,textcomments,target))
        db_connection.commit()


if __name__ == '__main__':
    # Call on the last Task in the graph.
    luigi.run(["--local-scheduler"], main_task_cls=ExportSqlDB)
