# **Streaming with PubSub, Dataflow and FAST API**

![image](https://user-images.githubusercontent.com/55681442/117441880-37092700-af60-11eb-9501-59f0b16449cd.png)

This file is my submission for week 4 from blankspace.io academi. This task is about how to create ETL to maintain request from user to editing database.
we build API using FAST API for user request modifying database and use pub sub to create stream process to big query.

## 1. Installation Instruction 
a. Download Gatekeeper.py, extract in one folder
    
b. Make sure you have all package pyhton for this program (see details in requirement.txt)

c. Create dataflow in google cloud platform using dataflow template Pub/Sub topics to big query

![image](https://user-images.githubusercontent.com/55681442/117424986-8cd3d400-af4c-11eb-81b7-3e5b2b8c6aa8.png)

d. Create pub sub topics

![image](https://user-images.githubusercontent.com/55681442/117425695-3dda6e80-af4d-11eb-85bf-88e4122a576b.png)

e. create dataset with name pubsub in bigquery, and make table activities2 with spesific schema (see in screen shots)

![image](https://user-images.githubusercontent.com/55681442/117425972-901b8f80-af4d-11eb-89a3-dc6393bbe454.png)


## 2. How to use 

a. After complete installation step, open folder and run the file using command **uvicorn gatekeeper:app --reload**
    
b. Open browser and use this url **http://127.0.0.1:8000/docs**

![image](https://user-images.githubusercontent.com/55681442/117424409-f43d5400-af4b-11eb-9046-3d36b37a039f.png)
    
c. Make POST request using this sample structure.

[

    {
    
        "operation": "insert",
        
        "table": "table1",
        
        "col_names": ["a","b","c"],
        
        "col_types": ["INTEGER","TEXT","TEXT"],
        
        "col_values": [1,"Backup and Restore", "2018-03-27 11:58:28.988414"]
        
        },
        
    {
    
        "operation": "delete",
        
        "table": "table1",
        
        "old_value": 
        
            {
            
                "col_names": ["a", "c"],
                
                "col_types": ["INTEGER", "TEXT"],
                
                "col_values": [3, "2019-04-28 10:24:30.183414"]
                
            }
            
    }
    
]

d. Data in pubsub dataset will be change after POST request sending

![image](https://user-images.githubusercontent.com/55681442/117426471-2059d480-af4e-11eb-92d4-ca0483b5ad3e.png)

