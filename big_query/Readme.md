# Audasa Big Query

## Code 
### Window
File to create window info with agregate info. 
* IMH .- Sum cars
* Time Travel .- Min 

### BigQuery
Functions to load big query table:
* Raw .- Car data
* Aggr .- Aggregate time

### Loader
Load from MySql in batch or streaming mode

Use:
* bigquery.py -f filename .- Load a csv file.
* loader.py Date .- Load data from mysql in batch mode
* loader.py Date online .- Load in streamind mode.
* loader.py Date --until_yestarday .- Load until yestarday

### env.py
Info about MySql server and BigQuery project.
You need to fill this file.


## User Test
Test big_query and window.
```Console
make test
```

