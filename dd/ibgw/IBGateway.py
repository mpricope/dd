from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.connection import Connection
from ibapi.utils import *  # @UnusedWildImport
from ibapi.common import *  # @UnusedWildImport
from ibapi.contract import Contract
import threading


class IBGateway(EClient, EWrapper):
	def __init__(self):
		EClient.__init__(self, self)
		self.events = {}
		self.nextRequest = 0
		self.lock = threading.Lock()

	def nextReqId(self):
		with self.lock:
			self.nextRequest += 1
			return self.nextRequest

	def reqFundamentalData(self, reqId: TickerId, contract: Contract,
							reportType: str, fundamentalDataOptions: TagValueList):
		super().reqFundamentalData(reqId, contract, reportType, fundamentalDataOptions)

	def error(self, reqId:TickerId, errorCode:int, errorString:str):
		super().error(reqId, errorCode, errorString)
		if reqId in self.events:
			obj = self.events[reqId]
			obj['result'] = None
			event = obj['event']
			event.set()

	def fundamentalData(self, reqId: TickerId, data: str):
		"""This function is called to receive fundamental
		market data. The appropriate market data subscription must be set
		up in Account Management before you can receive this data."""
		##self.logAnswer(current_fn_name(), vars())
		obj = self.events[reqId]
		obj['result'] = data
		event = obj['event']
		event.set()

	def getFundamentalData(self, contract: Contract, reportType: str):
		reqId = self.nextReqId()
		event = threading.Event()
		self.events[reqId] = {
			"event": event,
			"contract": contract,
			"result": None
		}
		self.reqFundamentalData(reqId, contract, reportType, [])
		event.wait()
		return self.events[reqId]['result']

