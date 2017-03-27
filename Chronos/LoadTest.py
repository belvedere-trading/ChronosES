#pylint: skip-file
from Chronos.Core import Aggregate, Event, ChronosIndex
from LoadTest_pb2 import LoadTest, CreateEvent, TestEvent

class LoadTestIndex(ChronosIndex):
    pass

class LoadTest(Aggregate):
    Proto = LoadTest
    Index = LoadTestIndex

    def IsValid(self):
        return True

class CreateEvent(Event):
    Aggregate = LoadTest
    Proto = CreateEvent

    def RaiseFor(self, aggregate):
        aggregate.createdTime = self.startTime
        aggregate.prevTime = self.startTime
        aggregate.currTime = self.startTime

class TestEvent(Event):
    Aggregate = LoadTest
    Proto = TestEvent

    def RaiseFor(self, aggregate):
        aggregate.prevTime = aggregate.currTime
        aggregate.currTime = self.sentTime
