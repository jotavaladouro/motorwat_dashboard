# Motorway DashBoard
Proof of concept: Dashboard for a motorway using dash, mapbox, Google Big Query and Google App Engine.

* Load data from MySql database to Big Query into raw data and aggregate date.
* Show a dashboard using Mapbox with AHI (average hourly intensity) and travel time using a window rolling.
* Show historical data and real-time data.
* Ready to deploy in App Engine Standar python 3.7.

    
![Motorway Dashboard](screenshot.png?raw=true "Motorway Dashboard")

## Big query database

### raw table
Table to store transits info

| Column      | Description|
|-------------|------------|
| N_Message | Daily index|
| N_Station|	Departure station|
| N_Lane	  | Departure lane |	
| D_Date	  | Transit Date |	
| T_Time	  | Transit Time |
| Sz_Key | Working key |
| N_Source  |	Path origin|
| N_Destination|	Path destination|
| N_Payment	| Payment method|
| N_Obu_Entry_Ok| OBU Entry info is valid|
| N_Obu_Payment	| OBU is valid |
| N_Obu_Entry_Station | Obu entry station | 	
| D_Obu_Entry_Date |  Obu entry date |
| T_Obu_Entry_Time | Obu entry time |	
| N_Obu_Entry_Lane	| Obu entry lane|
| Travel_Time_Second | Travel time in seconds|

### aggr table
Table to store information added by minutes.

| Column      | Description|
|-------------|------------|
| Source	| Path origin | 	
| Destination	| Path destination | 		
| Date	| date | 	
| Time	| Time |	
| AHI	| 	AHI |
| Travel_Time| Travel time in minutes|	


## Directories

* Dash
Dashboard and code to be served in app engine. Using dash python library

* big_query
Load data from MySql to BigQuery 

## Getting started

### Prerequisites
* [conda](https://anaconda.org/anaconda/python)
* [pip](https://pypi.org/project/pip/)
* python 3.7

### Instaling
* Clone the project
* Clone the environment
```console
conda create env -f motorway_dashboard.yml
```
* Create a google cloud platform account. [link](https://cloud.google.com/)
* Create a project in google cloud and activate billing [link](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
* Authenticate
```console
gcloud auth login
gcloud auth application-default login
```
* Modify biq_query/env.py with your data


* Mapbox

[Create an acount](https://www.mapbox.com/)

Fill dash/apps/env.py with your account


## Running test

### Test loader
```console
cd big_query
make test
```

### Run local app engine

```console
cd dash
make test
```

## Deployment
### Loader
```console
cd big_query
#Load data from last days
loader.py Date --until_yestarday
#Load data today online
loader.py Date --online
```
### App engine dashboard
```console
cd dash
make deploy
```

## Built with
* [dash](https://plot.ly/products/dash/)
* [mapbox](https://www.mapbox.com/mapbox-gl-js/style-spec/)
* [Google big query](https://cloud.google.com/bigquery/)
* [Google app engine](https://cloud.google.com/appengine/)
* [pandas_gbq](https://github.com/pydata/pandas-gbq)
* [import MySQLdb](https://sourceforge.net/projects/mysql-python/)


## Author
* Jose Manuel Fernandez Lorenzo - jotavaladouro-

# License
This project is licensed under the MIT License.





