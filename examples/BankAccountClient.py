from Chronos.Client import ChronosClient
from BankAccount_pb2 import BankAccount, CreateEvent, DepositEvent, WithdrawEvent, CloseEvent

class BankAccountClient(ChronosClient):
    EventTypes = (CreateEvent, DepositEvent, WithdrawEvent, CloseEvent)
    def __init__(self):
        super(BankAccountClient, self).__init__(BankAccount,
                                                callback=self.HandleSuccess,
                                                errorCallback=self.HandleFailure)
        self.numErrorsReceived = 0
        self.numEventsProcessed = 0

    def HandleSuccess(self, requestId, aggregateResponse, eventResponse, wasSentByMe):
        print 'Success: {0}: ${1}, {2}'.format(aggregateResponse.aggregate.owner,
                                               aggregateResponse.aggregate.balance,
                                               'Closed' if aggregateResponse.aggregate.isClosed else 'Open')

    def HandleFailure(self, chronosResponse, wasRequestSentByMe):
        print 'Failure: {0}'.format(chronosResponse.responseMessage)

    def CreateAccount(self, owner, amount):
        create = CreateEvent(owner=owner, amount=amount)
        self.RaiseEvent(create)

    def Deposit(self, accountId, amount):
        deposit = DepositEvent(amount=amount)
        self.RaiseEvent(deposit, accountId)

    def Withdraw(self, accountId, amount):
        withdraw = WithdrawEvent(amount=amount)
        self.RaiseEvent(withdraw, accountId)

    def CloseAccount(self, accountId):
        closeEvent = CloseEvent()
        self.RaiseEvent(closeEvent, accountId)
