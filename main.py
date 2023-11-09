# Script to connect on SQLServer and MySQL Databases, copying data from SQL Server to MySQL

# Some tips:
# If you get the error: pymssql._pymssql.OperationalError: (20009, b'DB-Lib error message 20009, severity 9:\nUnable to connect: Adaptive Server is unavailable or does not exist (<server>)\nDB-Lib error message 20009, severity 9:\nUnable to connect: Adaptive Server is unavailable or does not exist (<server>)\n')
# Be sure to be connected on VPN, if yes, dont worry, try again at least 5 times, sometimes the VPN get instabilities

import pymssql
import mysql.connector
from tqdm import tqdm

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
views = []

def testSQL():
    con = pymssql.connect(
        server=server,
        user=user,
        password=password,
        database=database,
        port=port
    )

    cursor = con.cursor()
    view_name = '<view name>'
    query = f"SELECT TOP 2 * FROM {view_name}"
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    con.close()

    # Print all columns
    for i in columns:
        print(i)

    # Print all data
    # for i in data:
    #     print(i)


def testMYSQL():
    cnx = mysql.connector.connect(**config)

    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            result = cursor.execute("SELECT * FROM employers LIMIT 5")
            rows = cursor.fetchall()
            for rows in rows:
                print(rows)
        cnx.close()
    else:
        print("Could not connect")


def tryConnectToSQLServer(attemps=0):
    try:
        source_conn = pymssql.connect(
            server=server,
            user=user,
            password=password,
            database=database,
            port=port
        )

        return source_conn
    except pymssql.InterfaceError:
        print("A MSSQLDriverException has been caught, trying again...")
    except pymssql.DatabaseError:
        print("A MSSQLDatabaseException has been caught, trying again...")
    except pymssql.OperationalError:
        print("A OperationalError has been caught, trying again...")

    if (attemps > 5):
        exit('Unable to connect to database, check the VPN, connection and database status.')
    return tryConnectToSQLServer(attemps + 1)


def main():
    # Establish connection to the source SQL Server database
    source_conn = tryConnectToSQLServer()

    # Establish connection to the target MySQL database
    target_conn = mysql.connector.connect(**config)

    # Create a cursor for the source connection
    source_cursor = source_conn.cursor()

    # Create a cursor for the target connection
    target_cursor = target_conn.cursor()

    # Loop through the views
    for view in views:
        # Query to retrieve data from the view
        select_query = f'SELECT * FROM {view}'

        # Execute the query on the source connection
        source_cursor.execute(select_query)

        # Get the column names from the cursor
        columns = [column[0] for column in source_cursor.description]

        # Create the target table using the same column names and data types
        script = f'CREATE TABLE IF NOT EXISTS {view} ({" varchar(255),".join(columns)} varchar(255))'
        target_cursor.execute(script)

        # Fetch all rows from the source cursor
        rows = source_cursor.fetchall()

        # Loop through the rows and insert them into the target table
        print(f"Init {view} data tranfer...")
        for row in tqdm(rows):
            # insert_query = f'INSERT INTO {view} VALUES ({", ".join(["?"] * len(row))})'
            columns_list = ','.join(columns)
            data_list = "', '".join(str(data).replace("'", "\\'") for data in row)
            insert_query = f"INSERT INTO {view} ({columns_list} ) VALUES ('{data_list}')"
            target_cursor.execute(insert_query)

    # Commit the changes to the target database
    target_conn.commit()

    # Close the cursors and connections
    source_cursor.close()
    target_cursor.close()
    source_conn.close()
    target_conn.close()

    print("Data transfer completed successfully.")


if __name__ == '__main__':
    main()
