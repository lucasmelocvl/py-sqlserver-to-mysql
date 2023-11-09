# py-sqlserver-to-mysql

Easy way to programmatically import table data from SQL Server database to MySQL. Usefull when it's necessary to schedule or use under VPN.

Before execute, remember to fill the variables above with yours database credentials and tables that you want to copy:
```
# Connection config for SQL Server Database
server = '<server>\\MSSQLSERVER'
user = '<user>'
password = '<password>'
database = '<database>'
port = '<port>'

# Connection config for MySQL Database
config = {
    'user': '<user>',
    'password': '<password>',
    'host': '<host>',
    'port': '<port>',
    'database': '<database>',
    'raise_on_warnings': True
}

# List of table name or views to retrieve data from
views = [<table name or view name>]
```

You are free to copy, fork and contribute on this script.

> NEXT STEPS: On create new table, all columns was setting with varchar(255), maybe on the future will be usefull create from-to datatype (mapping SQL Server datatypes compatible with MySQL datatypes) and create the column with the datatypes that best fit.