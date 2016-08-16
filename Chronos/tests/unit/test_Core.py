#pylint: skip-file
import mock
import unittest
import sys

class CoreBaseClassTest(unittest.TestCase):
    def setUp(self):
        googleMock = mock.MagicMock()
        protobufMock = googleMock.protobuf
        protobufMock.message = mock.MagicMock()
        protobufMock.message.Message = mock.MagicMock
        self.patcher = mock.patch.dict('sys.modules',
            {'Chronos.EventLogger': mock.MagicMock(),
             'Chronos.Chronos_pb2': mock.MagicMock(),
             'google': googleMock,
             'google.protobuf': protobufMock,
             'ordereddict': mock.MagicMock(),
             'sqlalchemy': mock.MagicMock(),
             'sqlalchemy.ext': mock.MagicMock(),
             'sqlalchemy.ext.declarative': mock.MagicMock(),
             'sqlalchemy.orm': mock.MagicMock()})
        self.patcher.start()

        global Aggregate, Event, MockProto, ChronosSemanticException, ChronosCoreException
        mockDescriptor = mock.MagicMock()
        fieldMock1 = mock.MagicMock()
        fieldMock1.name = 'indexTest1'
        fieldMock2 = mock.MagicMock()
        fieldMock2.name = 'indexTest2'
        mockDescriptor.fields = [fieldMock1, fieldMock2]
        class MockProto(mock.MagicMock):
            DESCRIPTOR = mockDescriptor
            indexTest1 = 'test1'
            indexTest2 = 'test2'

            SerializeToString = mock.MagicMock()
            ParseFromString = mock.MagicMock()
        MockProto.SerializeToString.return_value = 'protostring'
        MockProto.ParseFromString.return_value = MockProto()

        from Chronos.Core import Aggregate, Event, ChronosSemanticException, ChronosCoreException

    def tearDown(self):
        self.patcher.stop()

    def test_MultipleAggregateCreation(self):
        try:
            class TestAggregate(Aggregate):
                Proto = MockProto
            class AnotherAggregate(Aggregate):
                Proto = MockProto
        except ChronosSemanticException:
            pass
        else:
            self.fail('Expected ChronosSemanticException')


    def test_AggregateWithNoProto(self):
        try:
            class TestAggregate(Aggregate):
                pass
        except ChronosSemanticException:
            pass
        else:
            self.fail('Expected ChronosSemanticException')

    def test_AggregateWithBadProto(self):
        try:
            class TestAggregate(Aggregate):
                Proto = object
        except ChronosSemanticException:
            pass
        else:
            self.fail('Expected ChronosSemanticException')

    def test_AggregateWithInvalidExpiration(self):
        try:
            class TestAggregate(Aggregate):
                Proto = MockProto
                Expiration = 'bad'
        except ChronosSemanticException:
            pass
        else:
            self.fail('Expected ChronosSemanticException')

    def test_AggregateWithNegativeExpiration(self):
        try:
            class TestAggregate(Aggregate):
                Proto = MockProto
                Expiration = -123
        except ChronosSemanticException:
            pass
        else:
            self.fail('Expected ChronosSemanticException')

    def test_AggregateToDict(self):
        class TestAggregate(Aggregate):
            Proto = MockProto
        self.assertEqual(TestAggregate(1, 1, 5).ToDict(),
            {'aggregateId': 1, 'version': 1, 'proto': 'protostring', 'expiration': 5})
        self.assertEqual(MockProto.SerializeToString.call_count, 1)
        self.assertEqual(TestAggregate(2, 5, 6).ToDict(),
            {'aggregateId': 2, 'version': 5, 'proto': 'protostring', 'expiration': 6})
        self.assertEqual(MockProto.SerializeToString.call_count, 2)

    def test_AggregateFromDict(self):
        class TestAggregate(Aggregate):
            Proto = MockProto
        aggregate = TestAggregate.FromDict({'aggregateId': 1L, 'version': 1L, 'proto': 'anything', 'expiration': 2L})
        self.assertEqual(aggregate.aggregateId, 1L)
        self.assertEqual(aggregate.version, 1L)
        self.assertEqual(aggregate.expiration, 2L)
        self.assertTrue(isinstance(aggregate.proto, MockProto))
        self.assertEqual(MockProto.ParseFromString.call_count, 1)

    def test_EventWithNoMembers(self):
        try:
            class TestEvent(Event):
                pass

                def RaiseFor(self, aggregate):
                    pass
        except ChronosSemanticException:
            pass
        else:
            self.fail('Expected ChronosSemanticException')

    def test_EventWithNoProto(self):
        class TestAggregate(Aggregate):
            Proto = MockProto
        try:
            class TestEvent(Event):
                Aggregate = TestAggregate

                def RaiseFor(self, aggregate):
                    pass
        except ChronosSemanticException:
            pass
        else:
            self.fail('Expected ChronosSemanticException')

    def test_EventWithNoAggregate(self):
        try:
            class TestEvent(Event):
                Proto = MockProto

                def RaiseFor(self, aggregate):
                    pass
        except ChronosSemanticException:
            pass
        else:
            self.fail('Expected ChronosSemanticException')

    def test_EventWithBadAggregate(self):
        try:
            class TestEvent(Event):
                Proto = MockProto
                Aggregate = object

                def RaiseFor(self, aggregate):
                    pass
        except ChronosSemanticException:
            pass
        else:
            self.fail('Expected ChronosSemanticException')

    def test_EventWithAggregate(self):
        try:
            class TestEvent(Event):
                Proto = MockProto
                Aggregate = Aggregate

                def RaiseFor(self, aggregate):
                    pass
        except ChronosSemanticException:
            pass
        else:
            self.fail('Expected ChronosSemanticException')

    def test_EventWithNoRaiseFor(self):
        class TestAggregate(Aggregate):
            Proto = MockProto
        try:
            class TestEvent(Event):
                Aggregate = TestAggregate
                Proto = MockProto
        except ChronosSemanticException:
            pass
        else:
            self.fail('Expected ChronosSemanticException')

    def test_EventRaiseFor(self):
        class TestAggregate(Aggregate):
            Proto = MockProto
        class TestEvent(Event):
            Aggregate = TestAggregate
            Proto = MockProto

            def RaiseFor(self, aggregate):
                aggregate.indexTest1 = 'modified'

        aggregate = TestAggregate(1, 1, 10)
        TestEvent(1, 1).RaiseFor(aggregate)
        self.assertEqual(aggregate.indexTest1, 'modified')

    def test_EventFullName(self):
        class TestAggregate(Aggregate):
            Proto = MockProto
        class TestEvent(Event):
            Aggregate = TestAggregate
            Proto = MockProto

            def RaiseFor(self, aggregate):
                aggregate.indexTest1 = 'modified'

        self.assertEqual(TestEvent.fullName, 'unit.test_Core.TestEvent')


class AggregateRepositoryTest(unittest.TestCase):
    def setUp(self):
        googleMock = mock.MagicMock()
        protobufMock = googleMock.protobuf
        protobufMock.message = mock.MagicMock()
        protobufMock.message.Message = mock.MagicMock
        self.patcher = mock.patch.dict('sys.modules',
            {'Chronos.EventLogger': mock.MagicMock(),
             'Chronos.Chronos_pb2': mock.MagicMock(),
             'google': googleMock,
             'google.protobuf': protobufMock,
             'ordereddict': mock.MagicMock(),
             'sqlalchemy': mock.MagicMock(),
             'sqlalchemy.ext': mock.MagicMock(),
             'sqlalchemy.ext.declarative': mock.MagicMock(),
             'sqlalchemy.orm': mock.MagicMock()})
        self.patcher.start()

        mockDescriptor = mock.MagicMock()
        mockDescriptor.fields = []
        class MockProto(mock.MagicMock):
            DESCRIPTOR = mockDescriptor

        global ChronosSemanticException, ChronosCoreException, TestAggregate
        from Chronos.Core import Aggregate, ChronosCoreException, AggregateRepository
        class TestAggregate(Aggregate):
            Proto = MockProto
        self.mockEventStore = mock.MagicMock()
        self.mockIndexStore = mock.MagicMock()
        self.aggregateRepository = AggregateRepository(TestAggregate, self.mockIndexStore, self.mockEventStore)
        self.aggregateRepository.repository = {}

    def tearDown(self):
        self.patcher.stop()

    def test_Create(self):
        self.mockEventStore.GetAndIncrementAggregateId.return_value = 5
        aggregate = self.aggregateRepository.Create()
        self.assertEqual(aggregate.aggregateId, 5)
        self.assertEqual(aggregate.version, 1)
        self.assertEqual(aggregate.expiration, 0)
        self.assertEqual(self.aggregateRepository.repository, {})

    def test_Transaction_rollback(self):
        self.mockEventStore.TryGetSnapshot.return_value = None
        self.mockEventStore.GetAndIncrementAggregateId.side_effect = [1, 2, 3, 4]
        aggregate = self.aggregateRepository.Create()
        rolledBack = self.aggregateRepository.Create()
        rolledBack2 = self.aggregateRepository.Create()
        self.aggregateRepository.Emplace(aggregate)
        self.aggregateRepository.Begin()
        self.aggregateRepository.Emplace(rolledBack)
        self.aggregateRepository.Emplace(rolledBack2)
        self.assertEqual(self.aggregateRepository.Get(2), rolledBack)
        self.aggregateRepository.Rollback()
        self.assertRaises(ChronosCoreException, self.aggregateRepository.Get, 2)

    def test_Transaction_commit(self):
        self.mockEventStore.GetAndIncrementAggregateId.side_effect = [1, 2, 3, 4]
        aggregate = self.aggregateRepository.Create()
        rolledBack = self.aggregateRepository.Create()
        rolledBack2 = self.aggregateRepository.Create()
        self.aggregateRepository.Emplace(aggregate)
        self.aggregateRepository.Begin()
        self.aggregateRepository.Emplace(rolledBack)
        self.aggregateRepository.Emplace(rolledBack2)
        self.assertEqual(self.aggregateRepository.Get(2), rolledBack)
        self.aggregateRepository.Commit()
        self.aggregateRepository.Rollback()
        self.assertEqual(self.aggregateRepository.Get(3), rolledBack2)

    @mock.patch('time.time')
    def test_CreateWithExpiration(self, mockTime):
        mockTime.return_value = 1
        TestAggregate.Expiration = 123
        self.mockEventStore.GetAndIncrementAggregateId.return_value = 5
        aggregate = self.aggregateRepository.Create()
        self.assertEqual(aggregate.aggregateId, 5)
        self.assertEqual(aggregate.version, 1)
        self.assertEqual(aggregate.expiration, 124000000000)

    def test_GetWithUncachedAggregateId(self):
        self.mockEventStore.TryGetSnapshot.return_value = 'snapshot'

        mockAggregateClass = mock.MagicMock()
        mockAggregateClass.FromDict.return_value = 'snapshot'
        self.aggregateRepository.aggregateClass = mockAggregateClass

        aggregate = self.aggregateRepository.Get(1)
        self.assertEqual(aggregate, 'snapshot')

    def test_GetWithCachedAggregateId(self):
        self.aggregateRepository.repository[2] = 'cached'
        aggregate = self.aggregateRepository.Get(2)
        self.assertEqual(aggregate, 'cached')

    def test_GetShouldCacheAggregate(self):
        self.mockEventStore.TryGetSnapshot.return_value = 'snapshot'

        mockAggregateClass = mock.MagicMock()
        mockAggregateClass.FromDict.return_value = 'snapshot'
        self.aggregateRepository.aggregateClass = mockAggregateClass

        self.aggregateRepository.Get(1)
        aggregate = self.aggregateRepository.Get(1)
        self.assertEqual(aggregate, 'snapshot')
        self.assertEqual(self.mockEventStore.TryGetSnapshot.call_count, 1)

    def test_GetWithMissingAggregateIdShouldThrowException(self):
        self.mockEventStore.TryGetSnapshot.return_value = None
        self.assertRaises(ChronosCoreException, self.aggregateRepository.Get, 1)

    def test_GetAll(self):
        self.mockEventStore.GetAllSnapshots.return_value = 'aggregates'
        self.assertEqual(self.aggregateRepository.GetAll(), 'aggregates')
        self.mockEventStore.GetAllSnapshots.assert_called_once_with(TestAggregate)

    def test_GetFromIndex(self):
        self.mockEventStore.GetIndexedSnapshots.return_value = 'indexed_aggregates'
        self.mockIndexStore.RetrieveAggregateIds.return_value = [1, 2]
        self.assertEqual(self.aggregateRepository.GetFromIndex(indexKey='value'), 'indexed_aggregates')
        self.mockEventStore.GetIndexedSnapshots.assert_called_once_with(TestAggregate, [1, 2])


class EventProcessorTest(unittest.TestCase):
    def setUp(self):
        self.patcher = mock.patch.dict('sys.modules',
            {'Chronos.EventLogger': mock.MagicMock(),
             'Chronos.Chronos_pb2': mock.MagicMock(),
             'google': mock.MagicMock(),
             'google.protobuf': mock.MagicMock(),
             'ordereddict': mock.MagicMock(),
             'sqlalchemy': mock.MagicMock(),
             'sqlalchemy.ext': mock.MagicMock(),
             'sqlalchemy.ext.declarative': mock.MagicMock(),
             'sqlalchemy.orm': mock.MagicMock()})
        self.patcher.start()

        global ChronosCoreException, PersistenceBufferItem, PersistenceBufferManagementItem
        from Chronos.Core import (EventProcessor, ChronosCoreException, PersistenceBufferItem,
                                  AggregateLogicCompiler, IndexStore, PersistenceBufferManagementItem)
        from Chronos.Infrastructure import AbstractEventStore
        self.mockEventStore = mock.MagicMock(spec=AbstractEventStore)
        self.mockLogicCompiler = mock.MagicMock(spec=AggregateLogicCompiler)
        self.mockPool = mock.MagicMock()
        self.mockIndexStore = mock.MagicMock(spec=IndexStore)
        self.eventProcessor = EventProcessor(self.mockEventStore, self.mockLogicCompiler)
        self.eventProcessor.persistencePool = self.mockPool

    def tearDown(self):
        self.eventProcessor.persistencePool.terminate()
        self.patcher.stop()

    @mock.patch('time.time')
    def test_ProcessSuccess(self, mockTime):
        mockEvent = mock.MagicMock()
        mockEvent.version = 1
        mockAggregate = mock.MagicMock()
        mockAggregate.version = 1
        mockTime.return_value = 3
        self.eventProcessor.Process(mock.MagicMock(receivedTimestamp=1000), mockAggregate, mockEvent)
        mockEvent.RaiseFor.assert_called_once_with(mockAggregate)
        self.assertEqual(mockAggregate.version, 2)

    def test_ProcessWithMismatchingVersionsShouldFail(self):
        mockEvent = mock.MagicMock()
        mockEvent.version = 1
        mockAggregate = mock.MagicMock()
        mockAggregate.version = 2
        self.assertRaises(ChronosCoreException, self.eventProcessor.Process, mock.MagicMock(receivedTimestamp=1000),
                          mockAggregate, mockEvent)

    def test_ProcessIndexDivergence(self):
        self.eventProcessor.ProcessIndexDivergence('class', 123)
        item, = self.eventProcessor.persistenceBuffer
        self.assertEqual(item, self.mockPool.apply_async.return_value)

    def test_FlushPersistenceBufferWithEmptyBufferDoesNothing(self):
        self.eventProcessor.FlushPersistenceBuffer(mock.MagicMock)
        self.assertFalse(self.eventProcessor.persistenceBuffer)

    @mock.patch('time.time')
    def test_FlushPersistenceBufferWithBufferedItemsAndOverrideRemovesAll(self, mockTime):
        self.eventProcessor.EnqueueForPersistence('anything')
        self.eventProcessor.FlushPersistenceBuffer(mock.MagicMock, shouldForce=True)
        self.assertFalse(self.eventProcessor.persistenceBuffer)

    @mock.patch('time.time')
    def test_FlushPersistenceBufferWithoutOverideDoesNotRemoveItems(self, mockTime):
        self.eventProcessor.EnqueueForPersistence('anything')
        self.eventProcessor.FlushPersistenceBuffer(mock.MagicMock)
        self.assertEqual(len(self.eventProcessor.persistenceBuffer), 1)

    @mock.patch('time.time')
    def test_FlushPersistenceBufferBeyondLimitWithoutOverrideRemovesAll(self, mockTime):
        for _ in xrange(20):
            self.eventProcessor.EnqueueForPersistence('something')
        self.eventProcessor.FlushPersistenceBuffer(mock.MagicMock)
        self.assertFalse(self.eventProcessor.persistenceBuffer)

    @mock.patch('time.time')
    def test_Transaction_rollback(self, mockTime):
        self.eventProcessor.Begin()
        self.eventProcessor.ProcessIndexDivergence('class', 1)
        self.eventProcessor.ProcessFailure('req', 'class', 2)
        self.eventProcessor.ProcessTag(3, 'tag', 'expr', 1)
        self.eventProcessor.EnqueueForPersistence(4)
        self.eventProcessor.Rollback()
        self.eventProcessor.FlushPersistenceBuffer(mock.MagicMock, shouldForce=True)
        self.mockEventStore.PersistEvents.assert_called_once_with(mock.MagicMock, [])

    @mock.patch('time.time')
    def test_Transaction_commit(self, mockTime):
        self.eventProcessor.Begin()
        self.eventProcessor.ProcessIndexDivergence('class', 1)
        self.eventProcessor.ProcessFailure('req', 'class', 2)
        self.eventProcessor.ProcessTag(3, 'tag', 'expr', 1)
        self.eventProcessor.EnqueueForPersistence(4)
        self.assertEqual(len(self.eventProcessor.transactionBuffer), 4)
        self.eventProcessor.Commit()
        self.assertEqual(len(self.eventProcessor.persistenceBuffer), 4)
        self.eventProcessor.FlushPersistenceBuffer(mock.MagicMock, shouldForce=True)
        self.assertEqual(len(self.eventProcessor.persistenceBuffer), 0)
