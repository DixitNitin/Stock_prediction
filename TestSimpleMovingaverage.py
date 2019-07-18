from mysql import connector
import queue
import datetime
from decimal import *

mydb = connector.connect(user='root', password='iiit123',
                             host='127.0.0.1', auth_plugin='mysql_native_password',
                             database='screener')
mycursor = mydb.cursor()
sql = "use Screener"
mycursor.execute(sql)
sql = "SELECT timestamp, symbol, close FROM daily_data  where Symbol = 'COLPAL'"
mycursor.execute(sql)

trade = []#['symbol', datetime.date.today(), 0.0, 'BUY']

def Calculateprofit(trade):
	volume = 1
	isbuy = False
	buyprice = 0.0
	sellprice = 0.0
	buydate = datetime.date(2010,1,1)
	selldate = datetime.date(2010,1,1)
	profit = 0.0
	tradecount = 0
	tradedays = 0.0
	print (trade)
	for val in trade:
		if(val[3] == 'BUY'):
			if (isbuy):
				buyprice = ((volume * buyprice) + val[2] )/ (volume + 1)
				buydate += (val[1] - buydate) / (volume + 1)
				volume += 1
			else:
				profit = Decimal(profit) + (Decimal((Decimal(sellprice) - Decimal(val[2])) * Decimal(volume)))
				tradedays = Decimal(tradedays) + (Decimal((val[1] - selldate).days) * Decimal(volume))
				tradecount += volume
				buyprice = val[2]
				buydate = val[1]
				volume = 1
				isbuy = True
		if(val[3] == 'SELL'):
			if (isbuy):
				profit = Decimal(profit) + (Decimal((Decimal(val[2]) - Decimal(buyprice)) * Decimal(volume)))
				tradedays = Decimal(tradedays) + (Decimal((val[1] - buydate).days) * Decimal(volume))
				tradecount += volume
				sellprice = val[2]
				selldate = val[1]
				volume = 1
				isbuy = False
	
	print ('profit per trade : ' , profit/tradecount)
	#print ('Days per trade : ' , tradedays/tradecount)
	#print ('profit per trade per day : ' , (profit/tradecount)/(tradedays/tradecount))
	#print ('No of trade : ' , tradecount)
				
def MAtrigger(small, large):
	data = mycursor.fetchone()
	smalls = 0.0
	larges = 0.0
	smallup = True
	buyprice = 0.0
	print ( 'SELL')
	buydate = data[0]
	profit = 0.0
	trade_count = 0
	trade_days = 0
	smallq = queue.Queue(maxsize=small)
	largeq = queue.Queue(maxsize=large)
	while data is not None:
		if(smallq.full()):
			remove = smallq.get()
		else:
			remove = 0.0
		smallq.put(data[2])
		smalls = Decimal(smalls) - Decimal(remove) + Decimal(data[2])
		
		if(largeq.full()):
			remove = largeq.get()
		else:
			remove = 0.0
		largeq.put(data[2])
		larges = Decimal(larges) - Decimal(remove) + Decimal(data[2])
		if (largeq.full() == False):
			continue
		MAl = Decimal(larges)/Decimal(large)
		MAs = Decimal(smalls)/Decimal(small)
		if (smallup):
			if (MAl > MAs):
				#print (data[0], data[1], 'SELL')
				smallup = False
				if (buyprice != 0):
					profit = Decimal(profit) + Decimal(100.0)*((Decimal(data[2]) - Decimal(buyprice))/Decimal(buyprice))
					trade_days = Decimal(trade_days) + Decimal((data[0] - buydate).days)
					buyprice = 0.0
					trade_count = trade_count + 1
					trade.append([data[1], data[0], data[2], 'SELL'])
		else:
			if (MAl < MAs):
				#print (data[0], data[1], 'BUY')
				smallup = True
				if(buyprice == 0.0):
					buyprice = data[2]
					buydate = data[0]
					trade.append([data[1], data[0], data[2], 'BUY'])
				else:
					buyprice = 0.0
		data = mycursor.fetchone()
	print ('profit per trade : ' , profit/trade_count)
	print ('Days per trade : ' , trade_days/trade_count)
	print ('profit per trade per day : ' , (profit/trade_count)/(trade_days/trade_count))
	print ('No of trade : ' , trade_count)
	print ('Total profit : ' , profit)
	print ('\n'.join([ str(myelement) for myelement in trade ]))



MAtrigger(12.0,26.0)
Calculateprofit(trade)
mydb.close()