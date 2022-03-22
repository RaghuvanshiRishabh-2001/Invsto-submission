import pandas as pd
hinddata = pd.read_csv('HINDALCO.csv', index_col=False, delimiter = ',')
hinddata.head()


import mysql.connector as mysql
from mysql.connector import Error
# try:
#     conn = msql.connect(host='localhost', user='root',  
#                         password='root')#give ur username, password
#     if conn.is_connected():
#         cursor = conn.cursor()
#         cursor.execute("CREATE DATABASE INVSTO")
#         print("Database is created")
# except Error as e:
#     print("Error while connecting to MySQL", e)



try:
    conn = mysql.connect(host='localhost', database='invsto', user='root', password='root')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        cursor.execute('DROP TABLE IF EXISTS hindalco;')
        print('Creating table....')
        cursor.execute("CREATE TABLE hindalco(datetime datetime,close decimal(10,4),high decimal(10,4),low decimal(10,4),open decimal(10,4),volume int,instrument varchar(255))")
        print("Table is created....")

        for i,row in hinddata.iterrows():

            sql = "INSERT INTO invsto.hindalco VALUES (%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            # print("Record inserted")

            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)