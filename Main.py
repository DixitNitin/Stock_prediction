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
sql = "SELECT * FROM daily_data  where Symbol = 'MARICO'"

fulldf = pd.read_sql(sql,con=mydb)

WeightDict = {'SimpleMA' : 1,'awesome' : 1}


Awesome = Awesome.Calculate(5,34, 150, fulldf)
for i in Awesome:
	if(i[3]!=0):
		print(i)
mydb.close()