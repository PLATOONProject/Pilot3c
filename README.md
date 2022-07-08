# PLATOON Generator Mapping Rules Pilot3c

## Installation of Requirements 
```
pip install -r requeriments.txt
```

## Example of Importing the Functions
```
from generator.generator import generate_data,generate_forecast_data
generate_data("user","password","host","port","db", "table", tags,"start_date","end_date", resolution, sqlserver)
generate_forecast_data("user","password","host","port","db","table",tags,"date_saved","start_date","end_date",sqlserver)
```

## Parameters for generate_data 
- user: User for the server.
- password: Password for the server.
- host: Host for the server.
- db: Database in the server.
- table: Table of the database.
- tags: List of tags for the query.
- start_date: Start date for the query.
- end_date: End date for the query.
- resolution: Resolution for the query.
- sqlserver: Indicates if the MySQL database is SQL Server or not.

## Parameters for generate_forecast_data
- user: User for the server.
- password: Password for the server.
- host: Host for the server.
- db: Database in the server.
- table: Table of the database.
- tags: List of tags for the query.
- date_saved: Date in which the forecast data was saved. Is a parameter for the query.
- start_date: Start date for the query.
- end_date: End date for the query.
- sqlserver: Indicates if the MySQL database is SQL Server or not.