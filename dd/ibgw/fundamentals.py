import asyncio
from elasticsearch_dsl import connections, Q
from elasticsearch_dsl.query import *
import threading
import os.path
import sys
import time
import json
import datetime

from ibgw.IBGateway import IBGateway
from ibapi.contract import *
from parsers.stock import Stock

def run_ibg(ibg: IBGateway):
	ibg.connect("127.0.0.1", 7496, 2)
	ibg.run()

def main():
	stock = Stock.get(id="FR0000035370")
	#ibg.reqMatchingSymbols(20, stock.symbol)
	c = stock.contract()
	#c.primaryExchange = "SBF"
	result = ibg.getFundamentalData(stock.contract(), "ReportsFinStatements")
	f = open("data/" + stock.symbol + ".xml", "w")
	f.write(result)
	f.close()
	print(result)

def getFundamentalData(stock: Stock):
	fName = 'data/' + stock.isin + ".xml"
	if (os.path.isfile(fName)):
		with open(fName, "r") as f:
			ret = f.read()
		if ret == '':
			return None
		else:
			return ret
	time.sleep(10)
	ret = ibg.getFundamentalData(stock.contract(), "ReportsFinStatements")
	with open(fName, "w") as f:
		if ret == None:
			f.write('')
		else:
			f.write(ret)
	return ret

	
def pollIB():
	s = Stock.search()
	s.query = Bool(must_not=[Q('exists', field='lastFinPoll'), Match(notValid=True)])
	print(json.dumps(s.query.to_dict()))
	#sit = iter(s.scan())
	#for i in range(0,10):
	for stock in s.scan():
		#stock = next(sit)
		print(stock.symbol, stock.isin)
		result = getFundamentalData(stock)
		if result == None:
			print("Invalid")
			stock.notValid = True
		else:
			stock.parseFinancials(result)
			stock.lastFinPoll = datetime.datetime.now()
		stock.save()
		# print(result)


if __name__ == "__main__":
	ibg = IBGateway()
	x = threading.Thread(target=run_ibg, args=(ibg,))
	x.start()
	connections.create_connection()
	time.sleep(5)

	#main()
	pollIB()
	ibg.disconnect()
