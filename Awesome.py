# Find major minor trends, lower low and higher high is bear unless rveerse. Find reverse and vice versa
from mysql import connector
import queue
import datetime
import utility
from decimal import *


# output : date, score
#awesometrigger: MA on ehich we will trigger buy/sell
def Calculate(minor_period, major_period, awesomeTrigger, mycursor):
	fulldata = mycursor.fetchall()
	dailyAwesome = []
	weeklyAwesome = []

	weeklyFulldata = utility.convertToWeekly(fulldata)

	dailyMedian = utility.CalculateMedian(fulldata)
	weeklymedian = utility.CalculateMedian(weeklyFulldata)
	
	dailyAwesome = awesomeCalculator(dailyMedian, minor_period, major_period)
	weeklyAwesome = awesomeCalculator(weeklymedian, minor_period, major_period)
	
	return DailyAwesomeIndicator(fulldata, dailyAwesome, weeklyAwesome, awesomeTrigger)
	
	
# medianList is 2d array [timestamp, median]
def awesomeCalculator(medianList, minor_period, major_period):
	minorq = queue.Queue(maxsize=minor_period)
	majorq = queue.Queue(maxsize=major_period)
	major_sum = 0.0
	minor_sum = 0.0
	awesome = []
	for i in medianList:
		# Update minor
		if(minorq.full()):
			remove = minorq.get()
		else:
			remove = 0.0
		minorq.put(i[1])
		minor_sum = Decimal(minor_sum) + (Decimal(Decimal(i[1]) - Decimal(remove)))
		
		
		# Update major
		if(majorq.full()):
			remove = majorq.get()
		else:
			remove = 0.0
		majorq.put(i[1])
		major_sum = Decimal(major_sum) + (Decimal(Decimal(i[1]) - Decimal(remove)))
		
		if(majorq.full()):
			score = Decimal(Decimal(minor_sum)/Decimal(minor_period)) - Decimal((Decimal(major_sum)/Decimal(major_period)))
			awesome.append([i[0], score])
	
	return awesome
		


# input data : id, time, symbol, open, high, low, close
#output : time, value
def DailyAwesomeIndicator(fulldata, dailyAwesome, weeklyAwesome, awesomeTrigger):
	MAtriggerq = queue.Queue(maxsize = awesomeTrigger)
	sum = 0
	currentMA = 0
	final = []
	# trend : 1: bull, -1:bear
	trend = 1.0
	previ = fulldata[0]
	triggerTrade = previ[3]
	isTriggerBuy = False
	daysToCheck = 3
	score = 0
	for i in fulldata:
		trend = FindTrend(i[1], dailyAwesome)
		if(MAtriggerq.full()):
			remove = MAtriggerq.get()
		else:
			remove = 0.0
		MAtriggerq.put(i[6])
		sum = Decimal(sum) + (Decimal(Decimal(i[6]) - Decimal(remove)))
		currentMA = Decimal(sum) / Decimal(awesomeTrigger)
		
		if ((isCrossOver(previ[6], i, currentMA))):
			if((trend > 0)):
				if(VerifyTrigger(utility.GetWeek(i[1]), weeklyAwesome, trend)):
					#print ("after verify trigger isTriggerBuy ", i[1], i[6], score)
					score = 100
					triggerTrade = i[6]
					isTriggerBuy = True
			
			if((trend < 0)):
				if(VerifyTrigger(utility.GetWeek(i[1]), weeklyAwesome, trend)):
					score = - 100
					triggerTrade = i[3]
					isTriggerBuy = False
		else:
			--daysToCheck
		if((daysToCheck > 0) & (isCrossed(isTriggerBuy, i, triggerTrade))):
			final.append([i[1], score, trend])
			daysToCheck = 3
			score = 0
		elif(daysToCheck < 0):
			score = 0
			daysToCheck = 3
		
		previ = i
	
	return final
				

#input: date, awesomeArray(with 0 index as date and val as 1st param)
def VerifyTrigger(date, awesomeArray, trend):
	elementsToStudy = queue.Queue(maxsize = 3)
	elementsToStudy.put(0)
	elementsToStudy.put(0)
	elementsToStudy.put(0)
	for i in awesomeArray:
		if(date == i[0]):
			break;
		if(elementsToStudy.full()):
			elementsToStudy.get()
		elementsToStudy.put(i[1])
	
	first = elementsToStudy.get()
	second = elementsToStudy.get()
	third = elementsToStudy.get()
	
	if((trend > 0) & (second > first) & (third > second)):
		return True
	if((trend < 0) & (second < first) & (third < second)):
		return True
	return False
	
def FindTrend(date, awesomeArray):
	for i in awesomeArray:
		if(date == i[0]):
			if (Decimal(i[1]) > Decimal(0.0)):
				return Decimal(1.0)
			else:
				return Decimal(-1.0)
				
	return Decimal(0.0)

# row : id, time, symbol, open, high, low, close
def isCrossOver(trigger, currRow, currentMA):
	# from above to below on close price
	if(trigger > currentMA):
		if(currRow[5] < currentMA):
			return True
		else:
			return False
	# from below to above on high price
	else:
		if(currRow[4] > currentMA):
			return True
		else:
			return False
			
def isCrossed(isTriggerBuy, currRow, triggerTrade):
	if(isTriggerBuy):
		if(currRow[6] > triggerTrade):
			return True
		return False
	else:
		if(currRow[6] < triggerTrade):
			return True
		return False
