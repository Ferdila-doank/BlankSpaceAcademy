import csv
import pymysql.cursors
from datetime import datetime, timedelta
import pandas as pd
from unidecode import unidecode
import emoji
class MySQLCreate(object):
    def __init__(self, host, user, password):
        self.connection = pymysql.connect(host=host,
                                     user=user,
                                     password=password,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

    def fill_db_temp_json_csv(self,out_file,json_source):
        self.create_table_temp_json_csv()
        self.write_to_db_temp_json_csv(json_source)
        self.write_table_to_csv_temp_json_csv(out_file)

    #Create the database and table temporary
    def create_table_temp_json_csv(self):
        with self.connection.cursor() as cursor:
            cursor.execute("create database IF NOT EXISTS Json;")
            cursor.execute("use Json;")
            cursor.execute("drop table if exists temp_Json")
            sqlQuery = "CREATE TABLE IF NOT EXISTS temp_Json (" \
                       "contributors text," \
                       "truncated text, " \
                       "textJson text," \
                       "is_quote_status text, " \
                       "in_reply_to_status_id text, " \
                       "in_reply_to_user_id text, " \
                       "id text, " \
                       "favorite_count text, " \
                       "entities longtext, " \
                       "retweeted text, " \
                       "coordinates text, " \
                       "source longtext, " \
                       "in_reply_to_screen_name text, " \
                       "id_str text, " \
                       "retweet_count text, " \
                       "metadata text, " \
                       "favorited text, " \
                       "retweeted_status text, " \
                       "user text, " \
                       "geo text, " \
                       "in_reply_to_user_id_str text, " \
                       "lang text, " \
                       "created_at text, " \
                       "in_reply_to_status_id_str text, " \
                       "place text, " \
                       "quoted_status_id text, " \
                       "quoted_status text, " \
                       "possibly_sensitive text, " \
                       "quoted_status_id_str text, " \
                       "extended_entities text " \
                       ") ENGINE=InnoDB;"
            cursor.execute(sqlQuery)

        self.connection.commit()

    #extract csv convert to mysql
    def write_to_db_temp_json_csv(self,json_source):
        with self.connection.cursor() as cursor:
            df = pd.read_json(json_source, lines=True, dtype='str')
            df.to_csv('Json.csv', index=False)
            with open('Json.csv', 'r', encoding="utf8") as raw_file:
                 raw_data = csv.reader(raw_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                 data_list = list(raw_data)[1:]
            for row in data_list: 
                 try:
                    contributors = row[0]
                    truncated = row[1].replace(',','')             
                    textJson = unidecode(row[2])
                    is_quote_status = row[3].replace(',','')   
                    in_reply_to_status_id = "{:.12f}".format(float(row[4])).replace('.000000000000','')
                    in_reply_to_user_id  = str(row[5]).replace('.0','')
                    id = row[6]
                    favorite_count = row[7]
                    entities = row[8]
                    retweeted = row[9]
                    coordinates = row[10]
                    source = row[11]                
                    source = source.replace('"',' ')        
                    in_reply_to_screen_name = row[12] 
                    id_str = row[13]                      
                    retweet_count= row[14]                 
                    metadata= row[15]                      
                    favorited= row[16]                     
                    retweeted_status= row[17]              
                    user= row[18]                         
                    geo= row[19]                          
                    in_reply_to_user_id_str= row[20]      
                    lang= row[21]                         
                    created_at= row[22]                    
                    in_reply_to_status_id_str= row[23]     
                    place= row[24]                         
                    quoted_status_id= row[25]              
                    quoted_status= row[26]                 
                    possibly_sensitive= row[27]            
                    quoted_status_id_str= row[28]          
                    extended_entities= row[29]  

                    sql_query = 'INSERT INTO temp_json (contributors,truncated,textJson,is_quote_status,in_reply_to_status_id,in_reply_to_user_id' \
                                ',id,favorite_count,entities,retweeted,coordinates,source,in_reply_to_screen_name,id_str,retweet_count,metadata,favorited' \
                                ',retweeted_status,user,geo,in_reply_to_user_id_str,lang,created_at,in_reply_to_status_id_str,place,quoted_status_id,quoted_status,possibly_sensitive,quoted_status_id_str,extended_entities) ' \
                                'values ("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}");'.format(contributors,truncated,textJson,is_quote_status,in_reply_to_status_id,in_reply_to_user_id,id,favorite_count,entities,retweeted,coordinates,source,in_reply_to_screen_name,id_str,retweet_count,metadata,favorited,retweeted_status,user,geo,in_reply_to_user_id_str,lang,created_at,in_reply_to_status_id_str,place,quoted_status_id,quoted_status,possibly_sensitive,quoted_status_id_str,extended_entities) 
                    cursor.execute(sql_query)
                 except:
                    pass
        self.connection.commit()

    # Mysql to csv
    def write_table_to_csv_temp_json_csv(self, out_file):
        with self.connection.cursor() as cursor:
            #First_Name,Last_Name,Gender,Country,Age_Date,id
            cursor.execute("select * from temp_json;")
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
                    user= unidecode(row['user'] )                        
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
        self.connection.commit()


if __name__ == '__main__':
    sqlCreate = MySQLCreate(host='localhost', user='root', password='boss')
    sqlCreate.fill_db_temp_json_csv('create-sql-db.csv','tweet_data.json')

