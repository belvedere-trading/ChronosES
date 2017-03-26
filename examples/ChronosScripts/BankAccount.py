from Chronos.Core import Aggregate, Event, ChronosIndex, ValidationError, Unique
from BankAccount_pb2 import BankAccount, CreateEvent, DepositEvent, WithdrawEvent, CloseEvent

class BankAccountIndex(ChronosIndex):
    attributes = ('owner',)
    constraints = (Unique('owner', name='ux_owner'),)

class BankAccount(Aggregate):
    Proto = BankAccount
    Index = BankAccountIndex

    def IsValid(self):
        # Called after each Event is applied, but before persistence
        # to ensure that the Aggregate instance is never persisted
        # in a bad state. For example, a negative balance or a
        # missing owner would be no good!
        return self.owner and self.balance >= 0

class CreateEvent(Event):
    Aggregate = BankAccount
    Proto = CreateEvent

    def RaiseFor(self, aggregate):
        # This is the method that ChronosES will apply
        # each time an Event is sent
        if self.version != 1:
            raise ValidationError('Account has already been created')
        aggregate.owner = self.owner
        aggregate.balance = self.amount

def _ensureCreated(event):
    if event.version == 1:
        raise ValidationError('Account has not been created')

def _ensureOpen(aggregate):
    if aggregate.isClosed:
        raise ValidationError('Account is closed')

class DepositEvent(Event):
    Aggregate = BankAccount
    Proto = DepositEvent

    def RaiseFor(self, aggregate):
        _ensureCreated(self)
        _ensureOpen(aggregate)
        if self.amount <= 0:
            raise ValidationError('Deposit amount must be greater than 0')
        aggregate.balance += self.amount

class WithdrawEvent(Event):
    Aggregate = BankAccount
    Proto = WithdrawEvent

    def RaiseFor(self, aggregate):
        _ensureCreated(self)
        _ensureOpen(aggregate)
        if self.amount <= 0:
            raise ValidationError('Withdrawl amount must be greater than 0')
        aggregate.balance -= self.amount

class CloseEvent(Event):
    Aggregate = BankAccount
    Proto = CloseEvent

    def RaiseFor(self, aggregate):
        _ensureCreated(self)
        _ensureOpen(aggregate)
        if aggregate.balance != 0:
            raise ValidationError('Cannot close an account with a balance greater than 0')
        aggregate.isClosed = True
