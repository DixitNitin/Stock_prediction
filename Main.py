from mysql import connector
import queue
import datetime
from decimal import *
import Awesome
import utility
import pandas as pd

mydb = connector.connect(user='root', password='iiit123',
                             host='127.0.0.1', auth_plugin='mysql_native_password',
                             database='screener')
mycursor = mydb.cursor()
sql = "use Screener"
mycursor.execute(sql)
#sql = "IF NOT EXISTS( SELECT * FROM daily_data WHERE AND COLUMN_NAME = 'awesome' BEGIN ALTER TABLE daily_data ADD awesome NULL END"
#mycursor.execute(sql)
sql = "SELECT * FROM daily_data  where Symbol = 'SBIN'"

fulldf = pd.read_sql(sql,con=mydb)

WeightDict = {'SimpleMA' : 1,'awesome' : 1}
def input_trade(row):
    if row['score'] > 0:
        val = "BUY"
    elif row['score'] < 0:
        val = "SELL"
    else:
        val = "NONE"
    return val

Awesome = Awesome.Calculate(5,34, 200, fulldf)
Awesome = pd.DataFrame(Awesome, columns = ['symbol', 'timestamp','price', 'score'])
Awesome['trade'] = Awesome.apply(input_trade, axis = 1)

for i, row in Awesome.iterrows():
	if(row['trade'] != 'NONE'):
		print (row)
#print (Awesome)
utility.Calculateprofit(Awesome[['symbol','timestamp','price','trade']])

mydb.close()