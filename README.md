# **ETL With LUIGI**

![image](https://user-images.githubusercontent.com/55681442/115102355-8ddc9b80-9f74-11eb-9764-76dbdf3e89e2.png)

This file is my submission for task from blankspace.io week 1 about ETL process with luigi.

## 1. Installation Instruction 

    a. Download 4 folder source in this github an place to your computer in one folder  
    
    b. Make sure you have all package pyhton for this program (see details in requirement.txt)
    
    c. Make sure you have Mysql (refer to reqirement.txt) and you can login in that Mysql (have username and password)
    
    d. set your variable environtment variabel add sytem variabel set variabel name PYTHONPATH and variable value to path your copy folder
   
## 2. How to use 

    a. After complete installation step, open folder and run the file python etl_pipeline_[reviews/xls/disaster/json)
    
    b. Change variabel host, user, password to your Mysql environtment
    
    c. run program with command python etl_pipeline_[reviews/xls/disaster/json).py
    
    d. if you successsfuly run this program you will get this information
    
    ===== Luigi Execution Summary =====

    Scheduled 2 tasks of which:
    * 2 ran successfully:
        - 1 CreateTableFromCsv()
        - 1 ExportSqlDB()

    This progress looks :) because there were no failed tasks or missing dependencies

    ===== Luigi Execution Summary =====
