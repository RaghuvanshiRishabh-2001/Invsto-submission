from sre_parse import fix_flags
from cv2 import FileStorage_UNDEFINED
import numpy as np
import pandas as pd
from pandas.io import sql

import matplotlib.pyplot as plt

plt.style.use('fivethirtyeight')

df= pd.read_csv('HINDALCO.csv', index_col=False, delimiter = ',')

df = df.set_index(pd.DatetimeIndex(df['datetime'].values))
# print(df.head())

plt.figure(figsize=(16,8))
plt.title('Close Price History', fontsize=18)
plt.plot(df['close'])
plt.xlabel('Date',fontsize=18)
plt.ylabel('Close Price', fontsize=18)
# plt.show()

# fn to calculate the simple moving average (SMA)
def SMA(data,timeperiod=30,column="close"):
    return data[column].rolling(window=timeperiod).mean()

#create two colms to store 20 day and 50 day SMA
df['SMA20']=SMA(df,20)
df['SMA50']=SMA(df,50)
df['Signal'] = np.where(df['SMA20'] > df['SMA50'],1,0)

df['Position'] = df['Signal'].diff()

df["Buy"]=np.where(df['Position']==1,df['close'],0)

df["Sell"]=np.where(df['Position']==-1,df['close'],0)


plt.figure(figsize=(16,8))
plt.title('Close Price History with Buys/Sell signal', fontsize=18)
plt.plot(df['close'],alpha=0.5,label="Close")
plt.plot(df['SMA20'],alpha=0.5,label="SMA20")
plt.plot(df['SMA50'],alpha=0.5,label="SMA50")
plt.scatter(df.index, df['Buy'], alpha=1, label="Buy Signal", marker = '^' , color='green')
plt.scatter(df.index, df['Sell'], alpha=1, label="Sell Signal", marker = 'v' , color='red')
plt.xlabel('Date',fontsize=18)
plt.ylabel('Close Price', fontsize=18)
plt.show()

# preprocessing before stroing into database
df['SMA20'] = df['SMA20'].fillna(0)
df['SMA50'] = df['SMA50'].fillna(0)
df['Position'] = df['Position'].fillna(0)
print(df['Signal'].unique())

#storing into csvfile
# df.to_csv("finalhindalco.csv")


#Storing back into MYSQL into a new table
import mysql.connector as mysql
from mysql.connector import Error

try:
    conn = mysql.connect(host='localhost', database='invsto', user='root', password='root')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        cursor.execute("CREATE TABLE hindSMA(datetime datetime,close  decimal(10,4),high decimal(10,4),low decimal(10,4),open decimal(10,4),volume int,instrument varchar(255),SMA20 decimal(14,7),SMA50 decimal(14,7),signals int,position int, buy decimal(14,7), sell decimal(14,7))")

        print("You're connected to database: ", record)
        
        for i,row in df.iterrows():
            # print("Record inserted",tuple(row))
            sql = "INSERT INTO invsto.hindSMA VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            

            conn.commit()
        print("Query executed")

except Error as e:
            print("Error while connecting to MySQL", e)