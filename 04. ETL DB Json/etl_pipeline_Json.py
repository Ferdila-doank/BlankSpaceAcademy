import luigi
from create_Json_db import MySQLCreate
from datetime import datetime
from unidecode import unidecode

# Luigi Task to populate a MySQL table with the data given in the sample dateset
class CreateTableFromCsv(luigi.Task):
    rundate = datetime.now().strftime('%Y-%m-%d %H%M%S')
    host = 'localhost'
    user = 'root'
    password = 'boss'
    database = 'json'
    table = 'json'
    out_file = 'sql-db-{}.csv'.format(str(rundate))
    csv_source = 'tweet_data.json'

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(self.out_file)

    def run(self):
        mysql_db = MySQLCreate(host=self.host,
                               user=self.user,
                               password=self.password)
        mysql_db.fill_db_temp_json_csv(self.out_file,self.csv_source)


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
        self.Temp_To_json(db_connection)
        self.write_table_to_csv(db_connection, self.out_file)

    @staticmethod
    def Temp_To_json(db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute("use json")
            cursor.execute("CREATE TABLE IF NOT EXISTS json (contributors text,truncated text,textJson text,is_quote_status text,in_reply_to_status_id text,in_reply_to_user_id text,id text,favorite_count text,entities longtext,retweeted text,coordinates text,source longtext,in_reply_to_screen_name text,id_str text,retweet_count text,metadata text,favorited text,retweeted_status text,user text,geo text,in_reply_to_user_id_str text,lang text,created_at text,in_reply_to_status_id_str text,place text,quoted_status_id text,quoted_status text,possibly_sensitive text,quoted_status_id_str text,extended_entities text);")
            cursor.execute(
                "INSERT INTO json (contributors,truncated,textJson,is_quote_status,in_reply_to_status_id,in_reply_to_user_id,id,favorite_count,entities,retweeted,coordinates,source,in_reply_to_screen_name,id_str,retweet_count,metadata,favorited,retweeted_status,user,geo,in_reply_to_user_id_str,lang,created_at,in_reply_to_status_id_str,place,quoted_status_id,quoted_status,possibly_sensitive,quoted_status_id_str,extended_entities) "
                "SELECT * FROM temp_json "
                "WHERE id NOT IN "
                "(SELECT id FROM json);")
 
        db_connection.commit()

    @staticmethod
    def write_table_to_csv(db_connection, out_file):
        with db_connection.cursor() as cursor:
            cursor.execute("select * from json;")
            rows = cursor.fetchall()
            with open(out_file, "w") as out:
                out.write("contributors,truncated,textJson,is_quote_status,in_reply_to_status_id,in_reply_to_user_id,id,favorite_count,entities,retweeted,coordinates,source,in_reply_to_screen_name,id_str,retweet_count,metadata,favorited,retweeted_status,user,geo,in_reply_to_user_id_str,lang,created_at,in_reply_to_status_id_str,place,quoted_status_id,quoted_status,possibly_sensitive,quoted_status_id_str,extended_entities")
                for row in rows:
                    out.write("\n")
                    contributors = row['contributors']
                    truncated = row['truncated'].replace(',','')             
                    textJson = unidecode(row['textJson'])
                    is_quote_status = row['is_quote_status'].replace(',','')   
                    in_reply_to_status_id = "{:.12f}".format(float(row['in_reply_to_status_id'])).replace('.000000000000','')
                    in_reply_to_user_id  = str(row['in_reply_to_user_id']).replace('.0','')
                    id = row['id']
                    favorite_count = row['favorite_count']
                    entities = unidecode(row['entities'])
                    retweeted = row['retweeted']
                    coordinates = row['coordinates']               
                    source = row['source'].replace('"',' ')        
                    in_reply_to_screen_name = row['in_reply_to_screen_name'] 
                    id_str = row['id_str']                      
                    retweet_count= row['retweet_count']                 
                    metadata= row['metadata']                      
                    favorited= row['favorited']                     
                    retweeted_status= row['retweeted_status']              
                    user= unidecode(row['user'])                        
                    geo= row['geo']                          
                    in_reply_to_user_id_str= row['in_reply_to_user_id_str']      
                    lang= row['lang']                         
                    created_at= row['created_at']                    
                    in_reply_to_status_id_str= row['in_reply_to_status_id_str']     
                    place= row['place']                         
                    quoted_status_id= row['quoted_status_id']              
                    quoted_status= row['quoted_status']                 
                    possibly_sensitive= row['possibly_sensitive']            
                    quoted_status_id_str= row['quoted_status_id_str']          
                    extended_entities= row['extended_entities']
                    out.write('{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}'.format(contributors,truncated,textJson,is_quote_status,in_reply_to_status_id,in_reply_to_user_id,id,favorite_count,entities,retweeted,coordinates,source,in_reply_to_screen_name,id_str,retweet_count,metadata,favorited,retweeted_status,user,geo,in_reply_to_user_id_str,lang,created_at,in_reply_to_status_id_str,place,quoted_status_id,quoted_status,possibly_sensitive,quoted_status_id_str,extended_entities))
        db_connection.commit()


if __name__ == '__main__':
    # Call on the last Task in the graph.
    luigi.run(["--local-scheduler"], main_task_cls=ExportSqlDB)
