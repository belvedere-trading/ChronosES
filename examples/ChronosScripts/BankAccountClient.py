from Chronos.Client import ChronosClient
from BankAccount_pb2 import BankAccount, CreateEvent, DepositEvent, WithdrawEvent, CloseEvent

class BankAccountClient(ChronosClient):
    # EventTypes must contain all possible types of Event subclasses that ChronosES can send
    # back to the client. These are used for automatic Event deserialization.
    EventTypes = (CreateEvent, DepositEvent, WithdrawEvent, CloseEvent)
    def __init__(self):
        super(BankAccountClient, self).__init__(BankAccount,
                                                callback=self.HandleSuccess,
                                                errorCallback=self.HandleFailure,
                                                redisConnection=None)

    def HandleSuccess(self, requestId, aggregateResponse, eventResponse, wasSentByMe):
        # aggregateResponse is Chronos.Client.AggregateResponse
        # eventResponse is Chronos.Client.EventResponse
        print 'Success: {0}: "{1}" ${2}, {3}'.format(aggregateResponse.aggregateId,
                                                     aggregateResponse.aggregate.owner,
                                                     aggregateResponse.aggregate.balance,
                                                     'Closed' if aggregateResponse.aggregate.isClosed else 'Open')

    def HandleFailure(self, chronosResponse, wasRequestSentByMe):
        # chronosResponse is Chronos.Chronos_pb2.ChronosResponse
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
