# Simple Python ETL Project
##### This repo for Implementing ETL Process Using Python to Learn Data Engineering

## Table Of Contents
- [Problem Description](#problem)
- [Prerequisite](#pre) 
- [Implementation](#imp)
- [Result](#res)

<a name="problem"></a>
## Problem Description
Extracting data from multiple source files in diffrent format. [`datasource.zip`](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip) contain `.CSV`, `.JSON`, and `.XML` files for used cars data which contain features named:

|car_model|year_of_manufacture|price|fuel|
|-|-|-|-|

So we are going to extract and transform raw data, then load into a target tables in the database.


<a name="pre"></a>
## Prerequisite
- Python (Pandas)
- SQL (SQL Server 2019)

<a name="imp"></a>
## Implementation
After analyzing source data, implementation of this project is divided into two steps. first is to implement target tables in the database depending on the following Entity-Relationship Diagram (ERD):

![schema](https://user-images.githubusercontent.com/47898196/196304133-9aaf3309-1d94-4f55-9dc7-8f2bd367d787.png)

```sql
Create Table FuelType(
	id tinyint primary key,
	fuel_type nvarchar(10)
)
```
```sql
create Table UsedCars(
	car_model nvarchar(20),
	year_of_manufacture smallint,
	price float,
	fuel_id tinyint references FuelType(id)  
)
```

Second is to implement python ETL process:

- #### Dependancies

Import used libraries
```python
import pandas as pd
import xml.etree.ElementTree as ET
import os
import datetime
import pyodbc 
```
- #### Extract

Extract functions that will extract data from multiple sources in diffrent formats (CSV, JSON, XML) using pandas dataframe
```python
def extract_CSV(path_to_csv):
    df = pd.read_csv(path_to_csv)
    return df
```
```python
def extract_JSON(path_to_json):
    df = pd.read_json(path_to_json, lines= True)
    return df
```
```python
def extract_XML(path_to_xml):
    df = pd.read_xml(path_to_xml,'/root/row')
    return df
```
```python
def extract(path_to_src_data):
    df = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])    
    for f in os.listdir(path_to_src_data):
        fpath = os.path.join(path_to_src_data,f)
        
        if f.endswith('csv'):            
            df = df.append(extract_CSV(fpath))

        elif f.endswith('json'):
             df = df.append(extract_JSON(fpath))

        elif f.endswith('xml'):
             df = df.append(extract_XML(fpath))
    return df
```

- #### Transform

This function will convert the column price to 2 decimal places and replace column fuel to the corresponding index using `enumerate()`. Then return transformed data and dict of unique columns replaced with its index, in our project fuel column only is transformed and returned as dict {fuel_type as a key : its index as a value }.

```python
def transform(data):
    #round price to 2 decimal places
    data['price'] = round(data['price'],2)
    
    #get unique values from fuel column
    uniqueFuel = data['fuel'].unique().tolist()
    
    #create dict of {oldValue : newValue} to replace fuelType with its corresponding index 
    uniqueFuel = dict((value,index+1) for index, value in enumerate(uniqueFuel))
    data['fuel'] = data['fuel'].map(uniqueFuel)
    
    return data, {'uniqueFuel' : uniqueFuel}
```

- #### Load

It’s time to load the data into the target tables created in our database.

```python
def load(data, uniqueDict, server, database, username, password):     
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

    cursor = conn.cursor()
    
    #inserting into FuelType target table
    for key in uniqueDict['uniqueFuel']:
        fuel_type = key  #{fuel_type as a key  : its index as a value }    
        index = uniqueDict['uniqueFuel'][key]            
        cursor.execute("Insert into Demo.dbo.FuelType(id,fuel_type) values(?,?)",index ,fuel_type)
        
    
    #inserting into UsedCars target table
    for index,row in data.iterrows():   
        cursor.execute("Insert into Demo.dbo.UsedCars(car_model,year_of_manufacture,price,fuel_id) values(?,?,?,?)",
                       row.car_model, row.year_of_manufacture, row.price, row.fuel)
    
    conn.commit()
    cursor.close()
```

- #### Logging 

This function is to monitor and keep track of ETL process and write down its details into a `.txt` file

```python
def log_message(message, log_file_path):    
    timestamp_format = '%d-%h-%Y %H:%M:%S:%f' #Day-Month-Year Hour:Minute:Second:MSecond
    now = datetime.datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file_path,"a") as f:
        f.write(f'{message}\t{timestamp}\n')
        f.close()
```

- #### ETL Process

Finally 
```python
def ETL(src_path, log_path, server, database, username, password):    
    try:
        log_message('ETL Process Started',log_path)
        log_message('Extract Phase Started',log_path)
        EData = extract(src_path)
        log_message('Extract Phase Ended',log_path)
        log_message('Transform Phase Started',log_path)
        TData, uniqueDict = transform(EData)
        log_message('Transform Phase Ended',log_path)
        log_message('Load Phase Started',log_path)
        load(TData, uniqueDict, server, database, username, password)
        log_message('Load Phase Ended',log_path)
        log_message('ETL Process Ended',log_path) 
        
    except Exception as e: 
        print(e)       

    else:
        print('ETL Done Successfully')
```

<a name="res"></a>
## Result
Calling `ETL(src_path, log_path, server, database, username, password)`

![image](https://user-images.githubusercontent.com/47898196/196304243-01ba7e5e-34f9-4e97-8a42-e3cfbb80e21d.png)

Query `UsedCars` Table: 
```sql
select * from UsedCars
```

![UsedCars](https://user-images.githubusercontent.com/47898196/196303713-34c0b277-6373-4d55-a193-034e6e00d534.png)

Query `FuelType` Table: 
```sql
select * from FuelType
```

![FuelType](https://user-images.githubusercontent.com/47898196/196305745-2471a24b-c5e3-40e9-ae08-8be603e4fa86.png)

Joining two tables:
```sql
select c.car_model, c.year_of_manufacture, c.price, f.fuel_type 
from UsedCars c
join FuelType f 
on c.fuel_id = f.id
```
![join](https://user-images.githubusercontent.com/47898196/196303809-75e064fc-69f0-4d6c-8d58-ce7d97df280d.png)

Log file:

![image](https://user-images.githubusercontent.com/47898196/196305157-18172f1c-5d0c-41be-8b20-7c8b388a7050.png)
