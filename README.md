# **ETL Using Google Cloud Platform and Airflow (source JSON file)**

This file is my submission for task from blankspace.io week 2 about ETL process with google cloud platform and airflow. 
In this task i try to make ETL process from bucket in google strorage and transform to google big query dataset and 
make scheduler with airflow to processing ETL.

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
       
![image](https://user-images.githubusercontent.com/55681442/115867077-cc86c000-a464-11eb-97b2-35dcfcd1da09.png)

d. go to google bigquery and create dataset sumTransaction 
        
![image](https://user-images.githubusercontent.com/55681442/115866309-af9dbd00-a463-11eb-8b9f-69d8c30a9008.png)
        
e. in dataset browsing create table Transaction, Transactiontemp
        
![image](https://user-images.githubusercontent.com/55681442/115866447-df4cc500-a463-11eb-9fca-ebf9c23e27c7.png)

![image](https://user-images.githubusercontent.com/55681442/115866515-f4c1ef00-a463-11eb-9a78-20ca2b1c6daf.png)
    
f. Create folder GCP and copy file from folder 01 Credentials GCP and 02 python for process DAG to this folder and upload to home data irflow. 
   for password folder 01 Credentials GCP please contact me.  
        
![image](https://user-images.githubusercontent.com/55681442/115866696-3b174e00-a464-11eb-99cd-686a94b9e404.png)

g. copy file from folder 03 DAG Files (DAG_02.py) to folder DAG air flow
    
![image](https://user-images.githubusercontent.com/55681442/115866762-51bda500-a464-11eb-8114-a93c4d126b19.png)
        
## 2. How to use 

a. Goto airflow webserver from composer you have create before 
    
![image](https://user-images.githubusercontent.com/55681442/115842031-9edf4e00-a447-11eb-946a-e23220f48106.png)
        
![image](https://user-images.githubusercontent.com/55681442/115866881-89c4e800-a464-11eb-9eb3-0d0aea2e3683.png)
        
b. try to running manual DAG using trigger dag
    
c. if dag completed you will get green circle in DAG run field 
    
![image](https://user-images.githubusercontent.com/55681442/115978065-53dc4c80-a5a7-11eb-9e11-fd5f6071d203.png)

d. in google big query you will see table transaction and transactiontemp fill from blankspace table events
        
![image](https://user-images.githubusercontent.com/55681442/115978004-e3353000-a5a6-11eb-9c42-554562aa4e33.png)
