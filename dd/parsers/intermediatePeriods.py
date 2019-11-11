from elasticsearch_dsl import connections, Q
from elasticsearch_dsl.query import *
from parsers.stock import Stock


def computeIntermediatePeriods(stock: Stock):
	print(stock.symbol)
	fins = stock.fins
	fins.sort(key=lambda fp: fp['@Type'] + ":" + fp['startDate'])
	stock.fins = fins

def runLambda(q:Q, f):
	connections.create_connection()
	s = Stock.search()
	r = s.query(q)
#	for stock in r.scan():
	stock = Stock.get(id="FR0000076887")
	f(stock)
	stock.save()


if __name__ == "__main__":
	print("Running Lambda")
	connections.create_connection()
	q = Bool(must=[Q('exists', field='lastFinPoll')], must_not=[Match(notValid=True)])
	runLambda(q, computeIntermediatePeriods)
