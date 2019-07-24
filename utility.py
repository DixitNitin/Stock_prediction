import pandas as pd
import datetime
from decimal import *

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)
		
		
# input data : id, time, symbol, open, high, low, close
# outout data: id, week, symbol, open, high, low, close	
def convertToWeekly(data):	
	weekly = []
	last_week = -1
		
	for i, row in data.iterrows():
		curr_week = GetWeek(row["timestamp"])
		if(curr_week != last_week):
			this_Week = row
			this_Week["timestamp"] = curr_week
			if(last_week != -1):
				weekly.append(this_Week)
			
			last_week = curr_week
		else:
			if(Decimal(row["high"]) > this_Week["high"]):
				this_Week["high"] = Decimal(row["high"])
			if(Decimal(row["low"]) < Decimal(this_Week["low"])):
				this_Week["low"] = Decimal(row["low"])
			this_Week["close"] = Decimal(row["close"])
	
	return pd.DataFrame(weekly)


# input data : id, time, symbol, open, high, low, close
# output : time : median
def CalculateMedian(data):
	median = []
	for i, row in data.iterrows():
		med = Decimal(Decimal(row["open"]) + Decimal(row["close"])) / 2
		median.append([row["timestamp"], med])
	
	return pd.DataFrame(median, columns = ['timestamp','median'])
	
def GetWeek(date):
	return Decimal(date.isocalendar()[0] * 100) + date.isocalendar()[1]


#trade : symbol, date, price, score
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
	
	#if(tradecount > 0):
	#	print ('profit per trade : ' , profit/tradecount)
	#	print ('Days per trade : ' , tradedays/tradecount)
	#	print ('profit per trade per day : ' , (profit/tradecount)/(tradedays/tradecount))
	#	print ('No of trade : ' , tradecount)