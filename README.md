# **Streaming with PubSub, Dataflow and FAST API**

![Capture](https://user-images.githubusercontent.com/55681442/115311115-4aaf4200-a199-11eb-87b9-cc0e79f5047a.JPG)

This file is my submission for week 4 from blankspace.io academi. This task is about how to create ETL to maintain request from user to editing database.
we build API using FAST API for user request modifying database and use pub sub to create stream process to big query.

## 1. Installation Instruction 

    a. Download JsonExtract.py and movies.zip (link in movies link file.txt) in this github an place to your computer in one folder  
    
    b. Make sure you have all package pyhton for this program (see details in requirement.txt)
    
    c. Don't extract movies.zip
   
## 2. How to use 

    a. After complete installation step, open folder and run the file python JsonExtract.py
    
    b. change argument in JsonExtract.py. pathfile to path file movies.zip and pathdest is path for output JSON
    
    c. After this program complete run you will found 1 json file in pathdest
       If you want to see files output download JsonExtract.7z
