import os.path
import sys
from elasticsearch_dsl import Document, Date, Text, Keyword, connections, Boolean, utils
from ibapi.contract import *
import xmltodict
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import datetime
import pandas

class Stock(Document):
	name = Text()
	isin = Text()
	symbol = Text()
	market = Text()
	currency = Text()
	lastFinPoll = Date()
	notValid = Boolean()

	class Index:
		# we will use an alias instead of the index
		name = 'tickers'
		# set settings and possibly other attributes of the index like
		# analyzers
		settings = {
			'number_of_shards': 1,
			'number_of_replicas': 0
		}

	def contract(self):
		contract = Contract()
		contract.symbol = self.symbol
		contract.secType = "STK"
		contract.currency = self.currency
		contract.exchange = "SMART"
		#contract.primaryExchange = "ENEXT"
		contract.secIdType = "ISIN"
		contract.secId = self.isin
		return contract
	
	def determineDates(self, fiscalPeriod):
		endDate = parse(fiscalPeriod['@EndDate'])
		fpStatement = fiscalPeriod['Statement']
		if (isinstance(fiscalPeriod['Statement'], list)):
			statHeader = fpStatement[0]['FPHeader']
		else:
			statHeader = fpStatement['FPHeader']
		periodLength = statHeader['PeriodLength']
		periodType = statHeader['periodType']['#text']
		rArgs = {}
		rArgs[periodType.lower()] = -int(periodLength)
		startDate = endDate + relativedelta(**rArgs)
		fiscalPeriod['endDate'] = endDate
		fiscalPeriod['startDate'] = startDate
		return fiscalPeriod

	def parseFinancials(self, finStat:str):

		stats = xmltodict.parse(finStat)
		#print(stats)
		FinancialStatements = stats["ReportFinancialStatements"]["FinancialStatements"]
		fins = []
		if FinancialStatements != None:
			if "AnnualPeriods" in FinancialStatements:
				if (isinstance(FinancialStatements["AnnualPeriods"]["FiscalPeriod"], list)):
					fins = fins + FinancialStatements["AnnualPeriods"]["FiscalPeriod"]
				else:
					fins = fins + [FinancialStatements["AnnualPeriods"]["FiscalPeriod"]]

			if ("InterimPeriods" in FinancialStatements) and (FinancialStatements["InterimPeriods"] != None):
				if (isinstance(FinancialStatements["InterimPeriods"]["FiscalPeriod"], list)):
					fins = fins + FinancialStatements["InterimPeriods"]["FiscalPeriod"]
				else:
					fins = fins + [FinancialStatements["InterimPeriods"]["FiscalPeriod"]]

		t = list(map(self.determineDates, fins))
		t.sort(key=lambda fp: fp['startDate'].strftime("%y-%m-%d") + ':' + fp['@Type'])
		self.fins = t


	def getMetric(self, date:Date, metric:str):
		if (len(self.fins) == 0):
			return None
		sp = list(filter(lambda fp: parse(
			fp['startDate']) <= date and parse(fp['endDate']) >=date , self.fins))
		if (len(sp) == 0):
			if (date < parse(self.fins[0]['startDate'])):
				return None
			if (date > parse(self.fins[len(self.fins) - 1]['endDate'])):
				fp = self.fins[len(self.fins) - 1]
		else:
			fp = sp[0]
		path = metric.split(".")
		stList = list(filter(lambda st: st['@Type'] == path[0], fp['Statement']))
		if len(stList) > 0:
			st = stList[0]
			lineItemList = list(filter(lambda l: l['@coaCode'] == path[1], st['lineItem']))
			if len(lineItemList) > 0:
				return float(lineItemList[0]['#text'])
			else:
				return 0.0
		else:
			return 0.0

	def getMetrics(self, dates:list, metrics:list):
		if ('fins' not in self) or (len(self.fins) == 0):
			return None

		dateIt = iter(dates)
		fpIt = iter(self.fins)

		cDate = next(dateIt)
		cFp = next(fpIt)
		mDict = {}
		ret = []
		lastRow = []
		isNew = True
		tuples = []

		while True:
			while (parse(cFp['endDate']) < cDate):
				try:
					cFp = next(fpIt)
					isNew = True
				except:
					break
			if isNew:
				isNew = False

				#print(type(cFp['Statement']))
				if (isinstance(cFp['Statement'], utils.AttrList)):
					statements = cFp['Statement']
				else:
					statements = [cFp['Statement']]
				for statement in statements:
					lineItems = statement['lineItem']
					for lineItem in lineItems:
						metricName = statement['@Type'] + '.' + lineItem['@coaCode']
						mDict[metricName] = lineItem['#text']
				row = []
				for m in metrics:
					if m in mDict:
						row.append(float(mDict[m]))
					else:
						row.append(0.0)
				lastRow = row
			else:
				row = lastRow
			tuples.append((cDate, self.symbol))
			ret.append(row)
			try:
				cDate = next(dateIt)
			except:
				break

		mindex = pandas.MultiIndex.from_tuples(tuples)
		df = pandas.DataFrame(data=ret, index=mindex, columns=metrics)
		return df



if __name__ == "__main__":
	with open("data/BLC.xml", "r") as f:
		finData = f.read()
	connections.create_connection()
	stock = Stock.get(id="FR0000076887")
	# stock.parseFinancials(finData)
	m = stock.getMetric(parse("2018-10-10"), 'INC.SREV')
	print(m)
	df = stock.getMetrics([parse("2018-10-10"), parse("2018-10-12")], ['INC.SREV'])
	print(df)
	# stock.parseFinancials(finData)
	# stock.save()



