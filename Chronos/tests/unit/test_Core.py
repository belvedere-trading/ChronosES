#pylint: skip-file
import mock
import unittest
import sys
from nose_parameterized import parameterized

class CoreBaseClassTest(unittest.TestCase):
    def setUp(self):
        googleMock = mock.MagicMock()
        protobufMock = googleMock.protobuf
        protobufMock.message = mock.MagicMock()
        protobufMock.message.Message = mock.MagicMock
        self.patcher = mock.patch.dict('sys.modules',
            {'Chronos.EventLogger': mock.MagicMock(),
             'Chronos.Map': mock.MagicMock(),
             'Chronos.Chronos_pb2': mock.MagicMock(),
             'google': googleMock,
             'google.protobuf': protobufMock,
             'ordereddict': mock.MagicMock()})
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

    def test_AggregateGetIndices(self):
        class TestAggregate(Aggregate):
            Proto = MockProto

        aggregate = TestAggregate(1, 1)
        aggregate.IndexedAttributes = set(['a','b','c'])
        aggregate.NoCaseAttributes = set()
        aggregate.a, aggregate.b, aggregate.c = 1, 2, 3
        self.assertEqual(aggregate.GetIndices(), {'a':'1', 'b':'2', 'c':'3'})

    def test_AggregateDivergence(self):
        class TestAggregate(Aggregate):
            Proto = MockProto

        aggregate = TestAggregate(1, 1)
        aggregate.IndexedAttributes = set(['a','b','c'])
        aggregate.NoCaseAttributes = set()
        aggregate.a, aggregate.b, aggregate.c = 1, 2, 3
        aggregate2 = TestAggregate(1, 1)
        aggregate2.IndexedAttributes = set(['a','b','c'])
        aggregate2.NoCaseAttributes = set()
        aggregate2.a, aggregate2.b, aggregate2.c = 1, 2, 3
        self.assertFalse(aggregate2.HasDivergedFrom(aggregate))

        aggregate.a = 'not1'
        self.assertTrue(aggregate2.HasDivergedFrom(aggregate))

    @mock.patch('Chronos.Core.json')
    @mock.patch('Chronos.Core.json_format')
    def test_ToRESTDict(self, jsonFormat, json):
        class TestAggregate(Aggregate):
            Proto = MockProto

        aggregate = TestAggregate(1, 1)
        json.loads = mock.MagicMock(return_value = 'json')
        jsonFormat.MessageToJson = mock.MagicMock(return_value = 'json')
        self.assertEqual(aggregate.ToRESTDict(), {'aggregateId': 1, 'version': 1, 'proto': 'json'})

    def test_AggregateToDict(self):
        class TestAggregate(Aggregate):
            Proto = MockProto
        self.assertEqual(TestAggregate(1, 1).ToDict(),
            {'aggregateId': 1, 'version': 1, 'proto': 'protostring'})
        self.assertEqual(MockProto.SerializeToString.call_count, 1)
        self.assertEqual(TestAggregate(2, 5).ToDict(),
            {'aggregateId': 2, 'version': 5, 'proto': 'protostring'})
        self.assertEqual(MockProto.SerializeToString.call_count, 2)

    def test_AggregateFromDict(self):
        class TestAggregate(Aggregate):
            Proto = MockProto
        aggregate = TestAggregate.FromDict({'aggregateId': 1L, 'version': 1L, 'proto': 'anything'})
        self.assertEqual(aggregate.aggregateId, 1L)
        self.assertEqual(aggregate.version, 1L)
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

        aggregate = TestAggregate(1, 1)
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
             'ordereddict': mock.MagicMock()})
        self.patcher.start()

        mockDescriptor = mock.MagicMock()
        mockDescriptor.fields = []
        class MockProto(mock.MagicMock):
            DESCRIPTOR = mockDescriptor

        global ChronosSemanticException, ChronosCoreException, TestAggregate
        from Chronos.Core import Aggregate, ChronosCoreException, AggregateRepository
        class TestAggregate(Aggregate):
            Proto = MockProto
        self.mockEventReader = mock.MagicMock()
        self.aggregateRepository = AggregateRepository(TestAggregate, self.mockEventReader)
        self.aggregateRepository.repository = {}

    def tearDown(self):
        self.patcher.stop()

    def test_Create(self):
        self.mockEventReader.GetNextAggregateId.return_value = 5
        aggregate = self.aggregateRepository.Create()
        self.assertEqual(aggregate.aggregateId, 5)
        self.assertEqual(aggregate.version, 1)
        self.assertEqual(self.aggregateRepository.repository, {})

    def test_Transaction_rollback(self):
        self.mockEventReader.TryGetSnapshot.return_value = None
        self.mockEventReader.GetNextAggregateId.side_effect = [1, 2, 3, 4]
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
        self.mockEventReader.GetNextAggregateId.side_effect = [1, 2, 3, 4]
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

    def test_TransactionInProgress(self):
        self.aggregateRepository.transactionRepository = 1
        self.assertTrue(self.aggregateRepository._isTransactionInProgress())
        self.aggregateRepository.transactionRepository = None
        self.assertFalse(self.aggregateRepository._isTransactionInProgress())

    def test_NoBeginTransactionInProgress(self):
        self.aggregateRepository.transactionRepository = {'not':'cleared'}
        self.aggregateRepository.transactionEvent.clear = mock.MagicMock()
        self.aggregateRepository.Begin()
        self.assertEquals(self.aggregateRepository.transactionRepository, {'not':'cleared'})
        self.assertFalse(self.aggregateRepository.transactionEvent.clear.called)

    def test_NoCommitTransactionInProgress(self):
        self.aggregateRepository.transactionRepository = {'not':'cleared'}
        self.aggregateRepository.transactionEvent.set = mock.MagicMock()

        # Eventhough the transaction repo is non-none we want to force _isTransactionInProgress to return false in order to prove it is not committed and set
        self.aggregateRepository._isTransactionInProgress = mock.MagicMock(return_value = False)
        self.aggregateRepository.Commit()
        self.assertEquals(self.aggregateRepository.transactionRepository, {'not':'cleared'})
        self.assertFalse(self.aggregateRepository.transactionEvent.set.called)

    def test_GetWithInvalidAggregateId(self):
        self.assertRaises(ChronosCoreException, self.aggregateRepository.Get, 0)

    def test_GetWithUncachedAggregateId(self):
        self.mockEventReader.TryGetSnapshot.return_value = 'snapshot'

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
        self.mockEventReader.TryGetSnapshot.return_value = 'snapshot'

        mockAggregateClass = mock.MagicMock()
        mockAggregateClass.FromDict.return_value = 'snapshot'
        self.aggregateRepository.aggregateClass = mockAggregateClass

        self.aggregateRepository.Get(1)
        aggregate = self.aggregateRepository.Get(1)
        self.assertEqual(aggregate, 'snapshot')
        self.assertEqual(self.mockEventReader.TryGetSnapshot.call_count, 1)

    def test_GetWithMissingAggregateIdShouldThrowException(self):
        self.mockEventReader.TryGetSnapshot.return_value = None
        self.assertRaises(ChronosCoreException, self.aggregateRepository.Get, 1)

    def test_GetAll(self):
        self.mockEventReader.GetAllSnapshots.return_value = 'aggregates'
        self.assertEqual(self.aggregateRepository.GetAll(), 'aggregates')
        self.mockEventReader.GetAllSnapshots.assert_called_once()


class EventProcessorTest(unittest.TestCase):
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
             'ordereddict': mock.MagicMock()})
        self.patcher.start()

        global ChronosCoreException, PersistenceBufferItem, PersistenceBufferManagementItem, TestAggregate
        from Chronos.Core import (EventProcessor, ChronosCoreException, PersistenceBufferItem,
                                  AggregateLogicCompiler, PersistenceBufferManagementItem, Aggregate)
        from Chronos.Infrastructure import AbstractEventPersister, AbstractLogicStore, AbstractEventReader
        mockDescriptor = mock.MagicMock()
        mockDescriptor.fields = []
        class MockProto(mock.MagicMock):
            DESCRIPTOR = mockDescriptor
        class TestAggregate(Aggregate):
            Proto = MockProto
        self.mockEventPersister = mock.MagicMock(spec=AbstractEventPersister)
        self.eventProcessor = EventProcessor(TestAggregate, self.mockEventPersister)

    def tearDown(self):
        self.patcher.stop()

    @mock.patch('time.time')
    def test_ProcessSuccess(self, mockTime):
        mockEvent = mock.MagicMock()
        mockEvent.version = 1
        mockAggregate = mock.MagicMock()
        mockAggregate.version = 1
        mockTime.return_value = 3
        logicId = 1
        self.eventProcessor.Process(mock.MagicMock(receivedTimestamp=1000), mockAggregate, logicId, mockEvent)
        mockEvent.RaiseFor.assert_called_once_with(mockAggregate)
        self.assertEqual(mockAggregate.version, 2)

    def test_ProcessWithMismatchingVersionsShouldFail(self):
        mockEvent = mock.MagicMock()
        mockEvent.version = 1
        mockAggregate = mock.MagicMock()
        mockAggregate.version = 2
        logicId =1
        self.assertRaises(ChronosCoreException, self.eventProcessor.Process, mock.MagicMock(receivedTimestamp=1000),
                          mockAggregate, logicId, mockEvent)

    @mock.patch('Chronos.Core.PersistenceBufferManagementItem')
    def test_ProcessIndexDivergence(self, bufferItem):
        self.eventProcessor.ProcessIndexDivergence(123)
        item, = self.eventProcessor.persistenceBuffer
        self.assertEqual(item, bufferItem().Serialize())

    def test_FlushPersistenceBufferWithEmptyBufferDoesNothing(self):
        self.eventProcessor.FlushPersistenceBuffer()
        self.assertFalse(self.eventProcessor.persistenceBuffer)

    @mock.patch('time.time')
    def test_FlushPersistenceBufferWithBufferedItemsAndOverrideRemovesAll(self, mockTime):
        self.eventProcessor.EnqueueForPersistence(mock.MagicMock())
        self.eventProcessor.FlushPersistenceBuffer(shouldForce=True)
        self.assertFalse(self.eventProcessor.persistenceBuffer)

    @mock.patch('time.time')
    def test_FlushPersistenceBufferWithoutOverideDoesNotRemoveItems(self, mockTime):
        self.eventProcessor.EnqueueForPersistence(mock.MagicMock())
        self.eventProcessor.FlushPersistenceBuffer()
        self.assertEqual(len(self.eventProcessor.persistenceBuffer), 1)

    @mock.patch('time.time')
    def test_FlushPersistenceBufferBeyondLimitWithoutOverrideRemovesAll(self, mockTime):
        for _ in xrange(20):
            self.eventProcessor.EnqueueForPersistence(mock.MagicMock())
        self.eventProcessor.FlushPersistenceBuffer()
        self.assertFalse(self.eventProcessor.persistenceBuffer)

    @mock.patch('time.time')
    def test_Transaction_rollback(self, mockTime):
        self.eventProcessor.Begin()
        self.eventProcessor.ProcessIndexDivergence(1)
        self.eventProcessor.ProcessFailure('req', 2)
        self.eventProcessor.ProcessTag(3, 'tag', 'expr', 1)
        self.eventProcessor.EnqueueForPersistence(4)
        self.eventProcessor.Rollback()
        self.eventProcessor.FlushPersistenceBuffer(shouldForce=True)
        self.mockEventPersister.PersistEvents.assert_called_once_with([])

    @mock.patch('Chronos.Core.PersistenceBufferTagItem')
    @mock.patch('Chronos.Core.PersistenceBufferFailureItem')
    @mock.patch('Chronos.Core.PersistenceBufferManagementItem')
    @mock.patch('time.time')
    def test_Transaction_commit(self, mockTime, managementItem, failureItem, tagItem):
        self.eventProcessor.Begin()
        self.eventProcessor.ProcessIndexDivergence(1)
        self.eventProcessor.ProcessFailure('req', 2)
        self.eventProcessor.ProcessTag(3, 'tag', 'expr', 1)
        self.eventProcessor.EnqueueForPersistence(mock.MagicMock())
        self.assertEqual(len(self.eventProcessor.transactionBuffer), 4)
        self.eventProcessor.Commit()
        self.assertEqual(len(self.eventProcessor.persistenceBuffer), 4)
        self.eventProcessor.FlushPersistenceBuffer(shouldForce=True)
        self.assertEqual(len(self.eventProcessor.persistenceBuffer), 0)

    def test_UpsertAggregateSnapshot(self):
        aggregate = 'fake_aggregate'
        self.mockEventPersister.UpsertAggregateSnapshot = mock.MagicMock()
        self.eventProcessor.UpsertAggregateSnapshot(aggregate)
        self.mockEventPersister.UpsertAggregateSnapshot.assert_called_once_with('fake_aggregate')
        self.assertEquals(self.eventProcessor.aggregateSnapshotBuffer, ['fake_aggregate'])
        self.assertEquals(self.eventProcessor.aggregateSnapshotTransactionBuffer, [])

    def test_UpsertAggregateSnapshotLostConnection(self):
        aggregate = 'fake_aggregate'
        self.mockEventPersister.UpsertAggregateSnapshot = mock.MagicMock()
        self.eventProcessor._ensureAggregateSnapshotConsistency = mock.MagicMock()
        self.eventProcessor.lostConnection = True
        self.eventProcessor.UpsertAggregateSnapshot(aggregate)
        self.eventProcessor._ensureAggregateSnapshotConsistency.assert_called_once()
        self.mockEventPersister.UpsertAggregateSnapshot.assert_called_once_with('fake_aggregate')
        self.assertEquals(self.eventProcessor.aggregateSnapshotBuffer, ['fake_aggregate'])
        self.assertEquals(self.eventProcessor.aggregateSnapshotTransactionBuffer, [])

    def test_UpsertAggregateSnapshotTransactionInProgress(self):
        aggregate = 'fake_aggregate'
        self.mockEventPersister.UpsertAggregateSnapshot = mock.MagicMock()
        self.eventProcessor._isTransactionInProgress = mock.MagicMock(return_value = True)
        self.eventProcessor.UpsertAggregateSnapshot(aggregate)
        self.mockEventPersister.UpsertAggregateSnapshot.assert_called_once_with('fake_aggregate')
        self.assertEquals(self.eventProcessor.aggregateSnapshotBuffer, [])
        self.assertEquals(self.eventProcessor.aggregateSnapshotTransactionBuffer, ['fake_aggregate'])

    # How to test recursion?
    # def test_UpsertBadAggregateSnapshot(self):
    #     aggregate = 'fake_aggregate'
    #     self.mockEventPersister.UpsertAggregateSnapshot = mock.MagicMock()
    #     self.eventProcessor._isTransactionInProgress = mock.MagicMock(return_value = True)
    #     self.eventProcessor.UpsertAggregateSnapshot(aggregate)
    #     self.mockEventPersister.UpsertAggregateSnapshot.assert_called_once_with('fake_aggregate')
    #     self.assertEquals(self.eventProcessor.aggregateSnapshotBuffer, [])
    #     self.assertEquals(self.eventProcessor.aggregateSnapshotTransactionBuffer, ['fake_aggregate'])
    #     self.assertTrue(self.eventProcessor.lostConnection)

    def test_EnsureAggregateSnapshotConsistency(self):
        self.eventProcessor.aggregateSnapshotBuffer = ['aggregate1', 'aggregate2']
        self.eventProcessor.aggregateSnapshotTransactionBuffer = ['aggregate3', 'aggregate4']
        calls = [mock.call('aggregate3'), mock.call('aggregate4'),
                 mock.call('aggregate1'), mock.call('aggregate2')]

        self.eventProcessor._ensureAggregateSnapshotConsistency()
        self.mockEventPersister.UpsertAggregateSnapshot.assert_has_calls(calls, )
        self.assertFalse(self.eventProcessor.lostConnection)


class ChronosConstraintTest(unittest.TestCase):
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
             're': mock.MagicMock()})
        self.patcher.start()

        global FakeConstraint, ChronosConstraint, ChronosCoreException
        from Chronos.Core import ChronosConstraint, ChronosCoreException

        class FakeConstraint(ChronosConstraint):
            def Create():
                pass
            def Drop():
                pass

        self.mockRegEx = mock.MagicMock()

    def tearDown(self):
        self.patcher.stop()

    @mock.patch('Chronos.Core.re')
    def test_Construction(self, re):
        self.mockRegEx.match.return_value = True
        re.compile.return_value = self.mockRegEx
        constraint = FakeConstraint('attribute1', 'attribute2', name='valid')
        self.assertEqual(constraint.name, 'valid')
        self.assertEqual(constraint.attributes, set(['attribute1', 'attribute2']))
        re.compile.assert_called_once_with(r'^[a-zA-Z0-9_]+$')

    def test_ConstraintNoName(self):
        self.assertRaises(ChronosCoreException, FakeConstraint)
        self.assertRaises(ChronosCoreException, FakeConstraint, None)

    @mock.patch('Chronos.Core.re')
    def test_ConstraintInvalidName(self, re):
        self.mockRegEx.match.return_value = False
        re.compile.return_value = self.mockRegEx
        self.assertRaises(ChronosCoreException, FakeConstraint, name='not_valid')

    @mock.patch('Chronos.Core.re')
    def test_ConstraintInvalidName(self, re):
        self.mockRegEx.match.return_value = True
        re.compile.return_value = self.mockRegEx
        constraint = FakeConstraint('attribute1', 'attribute2', name='valid')

    @mock.patch('Chronos.Core.re')
    def test_ConstraintEqualityName(self, re):
        self.mockRegEx.match.return_value = True
        re.compile.return_value = self.mockRegEx
        a1 = FakeConstraint('attribute1', 'attribute2', name='same')
        a2 = FakeConstraint('attribute1', 'attribute2', name='same')
        a3 = FakeConstraint('attribute1', 'attribute2', name='different')

        self.assertEqual(a1, a2)
        self.assertNotEqual(a1, a3)

    @mock.patch('Chronos.Core.re')
    def test_ConstraintEqualityAttributes(self, re):
        self.mockRegEx.match.return_value = True
        re.compile.return_value = self.mockRegEx
        b1 = FakeConstraint('attribute1', 'attribute2', name='same')
        b2 = FakeConstraint('attribute1', 'attribute2', name='same')
        b3 = FakeConstraint('attribute1', 'attribute3', name='same')

        self.assertEqual(b1, b2)
        self.assertNotEqual(b1, b3)


class DummyConstraint(object):
    def __init__(self, name):
        self.name = name


class ConstraintComparerTest(unittest.TestCase):
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
             'ordereddict': mock.MagicMock()})
        self.patcher.start()

        global ConstraintComparer
        from Chronos.Core import ConstraintComparer

        self.a = DummyConstraint("a")
        self.b = DummyConstraint("b")
        self.c = DummyConstraint("c")
        self.d = DummyConstraint("d")
        self.e = DummyConstraint("e")

    def tearDown(self):
        self.patcher.stop()

    @parameterized.expand([
        ([],[],'','', [], []),
        ([],[],'new','new', [], []),
        (['not'],['same'],'a','a', [], []),  #same serialized so do nothing, would never happen that serialized equal but constraints do not
        (['a'],[],'not','same', ['a'], []),
        (['a','b','c'],[],'not','same', ['a','b','c'], []),
        ([],['a'],'not','same', [], ['a']),
        ([],['a','b','c'],'not','same', [], ['a','b','c']),
    ])
    def test_basic_comparison(self, newConstraints, oldConstraints, serializedNew, serializedOld, expectedAdd, expectedRemove):
        comparer = ConstraintComparer(newConstraints, oldConstraints, serializedNew, serializedOld)
        divergence = comparer.UpdateConstraints()
        self.assertEqual(expectedAdd, divergence.constraintsToAdd)
        self.assertEqual(expectedRemove, divergence.constraintsToRemove)

    def test_index_divergence_all_new(self):
        comparer = ConstraintComparer([self.a, self.b], [self.c, self.d], "not", "same")
        divergence = comparer.UpdateConstraints()
        self.assertEqual(set([self.a, self.b]), set(divergence.constraintsToAdd))
        self.assertEqual(set([self.c, self.d]), set(divergence.constraintsToRemove))

    def test_index_divergence_some_new(self):
        comparer = ConstraintComparer([self.a, self.b], [self.b, self.c, self.d], "not", "same")
        divergence = comparer.UpdateConstraints()
        self.assertEqual(set([self.a]), set(divergence.constraintsToAdd))
        self.assertEqual(set([self.c, self.d]), set(divergence.constraintsToRemove))

    def test_index_divergence_one_modified(self):
        newB = DummyConstraint("b")
        comparer = ConstraintComparer([self.a, newB], [self.b, self.c, self.d], "not", "same")
        divergence = comparer.UpdateConstraints()
        self.assertEqual(set([self.a, newB]), set(divergence.constraintsToAdd))
        self.assertEqual(set([self.b, self.c, self.d]), set(divergence.constraintsToRemove))

    def test_index_divergence_many_modified(self):
        newA = DummyConstraint("a")
        newC = DummyConstraint("c")
        newE = DummyConstraint("e")
        comparer = ConstraintComparer([newA, newC, newE, self.b], [self.a, self.c, self.d, self.e,], "not", "same")
        divergence = comparer.UpdateConstraints()
        self.assertEqual(set([newA, newC, newE, self.b]), set(divergence.constraintsToAdd))
        self.assertEqual(set([self.a, self.c, self.d, self.e]), set(divergence.constraintsToRemove))
