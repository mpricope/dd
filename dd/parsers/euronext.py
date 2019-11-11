import csv
import argparse
from parsers.stock import Stock
from elasticsearch_dsl import Document, Date, Text, Keyword, connections
from elasticsearch.helpers import bulk
import requests
from io import StringIO


def to_dict(header, arr):
	if (len(header) != len(arr)):
		return None
	
	ret = {}
	for i in range(0, len(header)):
		ret[header[i]] = arr[i]
	return ret
	

class EuroNextParser:
	fileName = ''

	def __init__(self, fName:str = ''):
		self.fileName = fName

	def downloadAndParse(self):
		url = 'https://live.euronext.com/pd/data/stocks/download?mics=ALXB%2CALXL%2CALXP%2CXPAR%2CXAMS%2CXBRU%2CXLIS%2CXMLI%2CMLXB%2CENXB%2CENXL%2CTNLA%2CTNLB%2CXLDN%2CXESM%2CXMSM%2CXATL&display_datapoints=dp_stocks&display_filters=df_stocks'
		# data = {}
		#url = 'https://live.euronext.com/pd/data/stocks/download'
		data = {'args[initialLetter]': None, 'args[fe_type]': 'csv', 'args[fe_layout]': 'ver',
                    'args[fe_decimal_separator]': '.', 'args[fe_date_format]': 'd/m/y'}
		r = requests.post(url, data)
		#print(r.text)
		allLines = r.text.replace('\r','\n').split('\n')
		spamreader = csv.reader(
			allLines, delimiter=';', quotechar='"', dialect=csv.excel_tab)
		self.parseCsv(spamreader)

	def parseCsv(self, spamreader):
		it = iter(spamreader)
		header = next(it)
		docs = []

		for row in it:
			dictRow = to_dict(header, row)
			if (dictRow != None):
				s = Stock()
				s.name = dictRow['Name']
				s.isin = dictRow['ISIN']
				s.meta.id = s.isin
				s.symbol = dictRow['Symbol']
				s.market = dictRow['Market']
				s.currency = dictRow['Trading Currency']
				docs.append(s)

		bulk(connections.get_connection(), (d.to_dict(True) for d in docs))


	def parse(self):
		with open(self.fileName, newline='') as csvfile:
			spamreader = csv.reader(csvfile, delimiter=';', quotechar='"')
			self.parseCsv(spamreader)
	

if __name__ == "__main__":
	enParser = EuroNextParser()
	connections.create_connection()
	Stock.init()
	enParser.downloadAndParse()

