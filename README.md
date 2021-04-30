# **ETL Using Dataproc and Pyspark**

This file is my submission for task from blankspace.io week 3 about ETL process with using dataproc and pyspark.
In this case try to make pipeline to processing data from local strorage to google cloud storage without writing script 
repeatedly

## 1. Installation Instruction 

a. Download 4 folder source in this github an place to your computer in one folder  
    
b. Make sure you have all package pyhton for this program (see details in requirement.txt)
    
c. Create bucket in google cloud storage make sure you have 3 main folder (Data, Dataproc, Output)
       
![image](https://user-images.githubusercontent.com/55681442/116665554-1ec76400-a9c4-11eb-93a9-037fc7a48c5e.png)

d. in folder dataproc copy file data from 02. Dataproc Python Script, 
        
![image](https://user-images.githubusercontent.com/55681442/116665766-59c99780-a9c4-11eb-9577-cc20d4a23327.png)
        
e. create dataproc template with this spec

![image](https://user-images.githubusercontent.com/55681442/116665950-985f5200-a9c4-11eb-8911-8d1122b24f2a.png)

![image](https://user-images.githubusercontent.com/55681442/116666082-bdec5b80-a9c4-11eb-957d-2b141887c73a.png)
    
f. in local computer copy data from folder 01.Json Source and 03. Local python script

g. create task schedule in windows with this spec 

![image](https://user-images.githubusercontent.com/55681442/116666502-35ba8600-a9c5-11eb-908c-5315093133e5.png)

h. setup that schedule running once a day 

## 2. How to use 

a. Go to google dataproc run the template dataproc 
    
![image](https://user-images.githubusercontent.com/55681442/116666747-87fba700-a9c5-11eb-9327-1a8ce775b20f.png)

b. After template datproc run you will get output in your bucke in folder output

![image](https://user-images.githubusercontent.com/55681442/116666936-c5603480-a9c5-11eb-86fe-a5f37d507f2e.png)

