import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from elasticsearch_dsl import connections, Q
from elasticsearch_dsl.query import *
from parsers.stock import Stock
import pandas

class DataFrameGenerator:

	def __init__(self, start:datetime, end:datetime, fields:list):
		self.start = start
		self.end = end
		self.fields = fields

	def generateDateDimension(self):
		ret = []
		cDate = self.end
		while cDate > self.start:
			ret.append(cDate)
			cDate = cDate + relativedelta(months=-1)

		return list(reversed(ret))
	
	def generateDF(self):
		s = Stock.search()
		r = s.query(Bool(must=[Q('exists', field='lastFinPoll')], must_not=[Match(notValid=True)]))
		tuples = []
		dates = self.generateDateDimension()
		data = []
		for stock in r:
			print(stock.symbol)
			for d in dates:
				tuples.append((d, stock.symbol))
				row = []
				for f in self.fields:
					row.append(stock.getMetric(d, f))
				data.append(row)
					
		mindex = pandas.MultiIndex.from_tuples(tuples)
		df = pandas.DataFrame(data=data, index=mindex, columns=self.fields)
		return df
		
	def generateDF2(self):
		s = Stock.search()
		r = s.query(Bool(must=[Q('exists', field='lastFinPoll')],
                   must_not=[Match(notValid=True)]))
		tuples = []
		dates = self.generateDateDimension()
		data = []
		i = 0
		for stock in r.scan():
			i = i + 1
			if i % 100 == 0:
				print(i)
			#print(stock.symbol)
			sdf = stock.getMetrics(dates, self.fields)
			data.append(sdf)

		# mindex = pandas.MultiIndex.from_tuples(tuples)
		# df = pandas.DataFrame(data=data, index=mindex, columns=self.fields)
		df = pandas.concat(data)
		return df



if __name__ == "__main__":
	connections.create_connection()
	dfg = DataFrameGenerator(
		parse("2018-01-01"), parse("2019-10-01"), ["INC.SREV", "INC.TIAT", "BAL.LLTD", "BAL.QTLE", "BAL.QTCO", "BAL.QTPO"])
	#df = dfg.generateDF()
	#print(df)
	df2 = dfg.generateDF2()
	#print(df2)
	#print(df.equals(df2))
	# summary = df.groupby(level=[0]).sum()
	summary = df2.groupby(level=[0]).sum()
	summary['ic'] = summary['BAL.LLTD'] + summary['BAL.QTLE']
	summary['roic'] = summary['INC.TIAT'] / summary['ic']
	# print(summary)
	summary.roic.plot(legend=True)
