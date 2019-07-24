# Find major minor trends, lower low and higher high is bear unless rveerse. Find reverse and vice versa
from mysql import connector
import queue
import datetime
import utility
from decimal import *
import pandas as pd
import numpy as np
import math


# output : symbol, date, price, score
#awesometrigger: MA on ehich we will trigger buy/sell
def Calculate(minor_period, major_period, awesomeTrigger, fulldf):
	dailyAwesome = []
	weeklyAwesome = []

	weeklyFulldata = utility.convertToWeekly(fulldf)

	dailyMedian = utility.CalculateMedian(fulldf)
	weeklymedian = utility.CalculateMedian(weeklyFulldata)
	
	dailyAwesome = awesomeCalculator(dailyMedian, minor_period, major_period)
	weeklyAwesome = awesomeCalculator(weeklymedian, minor_period, major_period)
	
	return DailyAwesomeIndicator(fulldf, dailyAwesome, weeklyAwesome, awesomeTrigger)
	
def awesomeCalculator(medianList, minor_period, major_period):
	medianList["MAsmall"] = medianList['median'].rolling(window=minor_period).mean()
	medianList["MAlarge"] = medianList['median'].rolling(window=major_period).mean()
	return medianList	

# input data : id, time, symbol, open, high, low, close
#output : symbol, date, price, score
def DailyAwesomeIndicator(fulldf, dailyAwesome, weeklyAwesome, awesomeTrigger):
	fulldf["MA"] = fulldf["close"].rolling(window=awesomeTrigger).mean()
	prevrow = fulldf.iloc[[1]]
	#print(fulldf)
	trend = 1
	daysToCheck = 3
	isTriggerBuy = False
	triggerTrade=prevrow
	final = []
	for i, row in fulldf.iterrows():
		trend = FindTrend(row['timestamp'], dailyAwesome)
		if ((isCrossOver(prevrow, row))):
			if(VerifyTrigger(utility.GetWeek(row['timestamp']), weeklyAwesome, trend)):
				score = trend * 100
				if(trend > 0):
					isTriggerBuy = True
				else:
					isTriggerBuy = False
				triggerTrade = row
		
		else:
			--daysToCheck
			if((daysToCheck > 0) & (isCrossed(isTriggerBuy, row, triggerTrade))):
				final.append([row['symbol'], row['timestamp'], row['close'],  score])
				daysToCheck = 3
				score = 0
			elif(daysToCheck < 0):
				score = 0
				daysToCheck = 3
			
		prevrow = row
	
	return final
				

#input: date, awesomeArray(with 0 index as date and val as 1st param)
def VerifyTrigger(date, awesomeArray, trend):
	elementsToStudy = queue.Queue(maxsize = 3)
	elementsToStudy.put(0)
	elementsToStudy.put(0)
	elementsToStudy.put(0)
	for i, row in awesomeArray.iterrows():
		if(date == row['timestamp']):
			if(math.isnan(row['MAlarge'])):
				return False
			break;
		if(elementsToStudy.full()):
			elementsToStudy.get()
		elementsToStudy.put(row['MAsmall'] - row['MAlarge'])
		
	first = elementsToStudy.get()
	second = elementsToStudy.get()
	third = elementsToStudy.get()
	
	if((trend > 0) & (second > first) & (third > second)):
		return True
	if((trend < 0) & (second < first) & (third < second)):
		return True
	return False
	
def FindTrend(date, awesomeArray):
	for i, row in awesomeArray.iterrows():
		if(date == row['timestamp']):
			if(math.isnan(row['MAlarge'])):
				return Decimal(0.0)
			if (Decimal(row['MAsmall']) > Decimal(row['MAlarge'])):
				return Decimal(1.0)
			else:
				return Decimal(-1.0)
				
	return Decimal(0.0)

# row : id, time, symbol, open, high, low, close
def isCrossOver(prevRow, currRow):
	if(math.isnan(prevRow['MA'])):
		return False

	if(Decimal(prevRow['close']) > Decimal(currRow['MA'])):
		if(Decimal(currRow['low']) < Decimal(currRow['MA'])):
			return True
		else:
			return False
	# from below to above on high price
	else:
		if(currRow['high'] > currRow['MA']):
			return True
		else:
			return False
			
def isCrossed(isTriggerBuy, currRow, triggerTrade):
	if(math.isnan(triggerTrade['MA'])):
		return False

	if(isTriggerBuy):
		if(Decimal(currRow["close"]) > Decimal(triggerTrade['close'])):
			return True
		return False
	else:
		if(Decimal(currRow["close"]) < Decimal(triggerTrade['open'])):
			return True
		return False