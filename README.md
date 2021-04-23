# **ETL With LUIGI**

![image](https://user-images.githubusercontent.com/55681442/115102355-8ddc9b80-9f74-11eb-9764-76dbdf3e89e2.png)

This file is my submission for task from blankspace.io week 2 about ETL process with google cloud platform.

## 1. Installation Instruction 

    a. Download 3 folder source in this github an place to your computer in one folder  
    
    b. Make sure you have all package pyhton for this program (see details in requirement.txt)
    
    c. set new project in google cloud platform and make composer elemen set up using this parameter.
       Location : asia-south1
       Zone : asia-south1-a
       Node count : 3
       Disk size (GB) : 20
       Machine type : n1-standard-1
       Cloud SQL machine type : db-n1-standard-2 (2 vCPU, 7.5 GB memory)
       Web server machine type : composer-n1-webserver-2 (2 vCPU, 1.6 GB memory)
       
       ![image](https://user-images.githubusercontent.com/55681442/115839939-5b83e000-a445-11eb-84c7-b654b92209b3.png)

    d. go to google bigquery and create dataset browsing 
        
        ![image](https://user-images.githubusercontent.com/55681442/115840319-bd444a00-a445-11eb-9566-1c401e6cc357.png)
        
    e. in dataset browsing create table Keyword, KeywordTemp and MostSearch
        
        ![image](https://user-images.githubusercontent.com/55681442/115840494-f54b8d00-a445-11eb-9ea5-32260ba57f48.png)

        ![image](https://user-images.githubusercontent.com/55681442/115840597-0c8a7a80-a446-11eb-9e65-d8e48485b7a0.png)
        
        ![image](https://user-images.githubusercontent.com/55681442/115840676-2035e100-a446-11eb-8081-9c532826ad9c.png)
    
    f. Create folder GCP and copy file from folder 01 Credentials GCP and 02 python for process DAG to this folder and upload to home data irflow. 
       for password folder 01 Credentials GCP please contact me.  
        
        ![image](https://user-images.githubusercontent.com/55681442/115841221-c5e95000-a446-11eb-89bd-01077a6b6253.png)

    g. copy file from folder 03 DAG Files (DAG_01.py) to folder DAG air flow
    
        ![image](https://user-images.githubusercontent.com/55681442/115841820-62abed80-a447-11eb-96ac-04781ba74931.png)
        
          
## 2. How to use 

    a. Goto airflow webserver from composer you have create before 
    
        ![image](https://user-images.githubusercontent.com/55681442/115842031-9edf4e00-a447-11eb-946a-e23220f48106.png)
        
        ![image](https://user-images.githubusercontent.com/55681442/115842297-e1088f80-a447-11eb-91e8-2fa5448bb3fb.png)
        
    b. try to running manual DAG using trigger dag
    
    c. if dag completed you will get green circle in DAG run field 
    
        ![image](https://user-images.githubusercontent.com/55681442/115842793-5f653180-a448-11eb-9c20-a5945172b016.png)

    d. in google big query you will see table keyword and most search fill from txt source in bucket
        
        ![image](https://user-images.githubusercontent.com/55681442/115843175-c1259b80-a448-11eb-92e5-9e7b35c64649.png)
