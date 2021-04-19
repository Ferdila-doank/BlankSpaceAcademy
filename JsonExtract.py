import zipfile
import pandas as pd

def ETLJson(pathfile,pathdest):
    with zipfile.ZipFile(pathfile, 'r') as zip:
        ZipMovies = zip.namelist()
        ListFile = []
        for i in ZipMovies:
            if i.find(".json") != -1:
                ListFile.append(i)

        print("get file done")
        ListFile.sort(reverse=True)

        dfjson = pd.DataFrame(columns=['original_title','budget','popularity','release_date','revenue','runtime' \
                                   ,'vote_average','vote_count','spoken_languages','genres'])

        for i in range(len(ListFile)):

            with zip.open(ListFile[i]) as jsonfile:
                try:
                    df = pd.read_json(jsonfile, lines=True)
                    df2 = pd.DataFrame([y for x in df['spoken_languages'].values.tolist() for y in x])
                    df3 = pd.DataFrame([y for x in df['genres'].values.tolist() for y in x])
                                        
                    print(ListFile[i])
                    print('original_title : ' + str(df['original_title'][0]))
                    print('budget : ' + str(df['budget'][0]))
                    print('popularity : ' + str(df['popularity'][0]))
                    print('release_date : ' + str(df['release_date'][0]))
                    print('revenue : ' + str(df['revenue'][0]))
                    print('runtime : ' + str(df['runtime'][0]))
                    print('vote_average : ' + str(df['vote_average'][0]))
                    print('vote_count : ' + str(df['vote_count'][0]))

                    spoken_languages = df2['name'].values
                    print(spoken_languages) 

                    genres = df3['name'].values
                    print(genres)

                    new_row = {'original_title': df['original_title'][0] , \
                               'budget': df['budget'][0] , \
                               'popularity': df['popularity'][0] , \
                               'release_date': df['release_date'][0] , \
                               'revenue': df['revenue'][0] , \
                               'runtime': df['runtime'][0] , \
                               'vote_average': df['vote_average'][0] , \
                               'vote_count': df['vote_count'][0] , \
                               'spoken_languages': spoken_languages , \
                               'genres': genres \
                              }
                    
                    dfjson = dfjson.append(new_row, ignore_index=True)
                except:
                    pass

    dfjson.to_json(pathdest, orient='records', lines=True)

ETLJson(pathfile="D:/BlankSpace.io/Code BlankSpace.io/SelfLearning1/movies.zip", \
        pathdest="D:/BlankSpace.io/Code BlankSpace.io/SelfLearning1/JsonExtract.json" \
        )
# Change pathfile to source file (movies.zip) and path dest to folder to save json file
