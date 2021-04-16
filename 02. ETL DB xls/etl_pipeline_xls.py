import luigi
from create_xls_db import MySQLCreate
from datetime import datetime
from unidecode import unidecode

# Luigi Task to populate a MySQL table with the data given in the sample dateset
class CreateTableFromCsv(luigi.Task):
    rundate = datetime.now().strftime('%Y-%m-%d %H%M%S')
    host = 'localhost'
    user = 'root'
    password = 'boss'
    database = 'xls'
    table = 'xls'
    out_file = 'sql-db-{}.csv'.format(str(rundate))
    csv_source = 'file_1000.xls'

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(self.out_file)

    def run(self):
        mysql_db = MySQLCreate(host=self.host,
                               user=self.user,
                               password=self.password)
        mysql_db.fill_db_temp_xls_csv(self.out_file,self.csv_source)


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
        self.Temp_To_xls(db_connection)
        self.write_table_to_csv(db_connection, self.out_file)

    @staticmethod
    def Temp_To_xls(db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("use xls")
            cursor.execute("CREATE TABLE IF NOT EXISTS xls (First_Name text,Last_Name text,Gender text,Country text,Age_Date date,id text);")
            cursor.execute(
                "INSERT INTO xls (First_Name,Last_Name,Gender,Country,Age_Date,id) "
                "SELECT * FROM temp_xls "
                "WHERE id NOT IN "
                "(SELECT id FROM xls);")
 
        db_connection.commit()

    @staticmethod
    def write_table_to_csv(db_connection, out_file):
        with db_connection.cursor() as cursor:
            cursor.execute("select * from xls;")
            rows = cursor.fetchall()
            with open(out_file, "w") as out:
                out.write("First_Name,Last_Name,Gender,Country,Age_Date,id")
                for row in rows:
                    out.write("\n")
                    First_Name = row['First_Name']
                    Last_Name = row['Last_Name']
                    Gender = row['Gender']
                    Country = row['Country']
                    Age_Date = row['Age_Date'].strftime('%Y-%m-%d')
                    id = row['id']
                    
                    out.write('{}, {}, {}, {}, {}, {}'.format(First_Name,Last_Name,Gender,Country,Age_Date,id))
        db_connection.commit()


if __name__ == '__main__':
    # Call on the last Task in the graph.
    luigi.run(["--local-scheduler"], main_task_cls=ExportSqlDB)
