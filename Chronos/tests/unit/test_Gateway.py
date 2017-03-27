#pylint: skip-file
import itertools
import mock
import unittest
import sys
from collections import namedtuple
from nose_parameterized import parameterized
from Queue import Queue
from threading import Lock

MockAggregate = namedtuple('MockAggregate', ['aggregateId', 'version', 'lock'])
class MockDivergedAggregate(MockAggregate):
    lock = Lock()
    def HasDivergedFrom(self, _):
        return True

class MockNonDivergedAggregate(MockAggregate):
    lock = Lock()
    def HasDivergedFrom(self, _):
        return False

class GatewayTestBase(object):
    def GetMockCoreProviderGenerator(self):
        self.mockNotifier = mock.MagicMock()
        self.mockAggregateRepository = mock.MagicMock()
        self.mockBufferItem = mock.MagicMock()
        self.mockEventProcessor = mock.MagicMock()
        self.mockEventProcessor.FlushPersistenceBuffer = lambda shouldForce: shouldForce
        self.mockEventProcessor.Process = lambda eventQueueItem, aggregate, logicId, event: (aggregate, self.mockBufferItem)
        self.mockCoreProvider = mock.MagicMock()
        mockCoreProviderGenerator = mock.MagicMock(return_value=self.mockCoreProvider)
        self.mockCoreProvider.GetNotifier.return_value = self.mockNotifier
        self.mockCoreProvider.GetRepository.return_value = self.mockAggregateRepository
        self.mockCoreProvider.GetProcessor.return_value = self.mockEventProcessor
        return mockCoreProviderGenerator

class ChronosProcessTest(unittest.TestCase, GatewayTestBase):
    def setUp(self):
        multiprocessingMock = mock.MagicMock()
        multiprocessingManagersMock = multiprocessingMock.managers
        multiprocessingManagersMock.BaseManager = mock.MagicMock
        multiprocessingManagersMock.BaseManager.register = mock.MagicMock()
        self.patcher = mock.patch.dict('sys.modules',
            {'Chronos.EventLogger': mock.MagicMock(),
             'Chronos.Chronos_pb2': mock.MagicMock(),
             'Chronos.Core': mock.MagicMock(),
             'multiprocessing': multiprocessingMock,
             'multiprocessing.managers': multiprocessingManagersMock,
             'time': mock.MagicMock(),
             'time.sleep': mock.MagicMock()
            })
        self.patcher.start()

        global ChronosGatewayException, ChronosEventQueueItem, ChronosQueryItem, ChronosEventQueueTransaction, ChronosFailedEventPersistenceException
        from Chronos.Gateway import (ChronosProcess, ChronosGatewayException, ChronosEventQueueItem, ChronosQueryItem,
                                    ChronosFailedEventPersistenceException, ChronosEventQueueTransaction, )
        self.mockAggregateClass = mock.MagicMock(__name__='TestName', IndexedAttributes=[])
        self.mockAggregateRegistry = mock.MagicMock(spec=dict)
        self.mockLogicMetadata = mock.MagicMock(logicId=1, logicVersion=1)
        self.mockCoreProviderGenerator = self.GetMockCoreProviderGenerator()
        self.mockProfiler = mock.MagicMock()
        self.chronosProcess = ChronosProcess(self.mockAggregateClass, self.mockAggregateRegistry, self.mockLogicMetadata,
                                             self.mockCoreProviderGenerator, self.mockProfiler)
        # Convenience so we don't have to call this in almost every test method
        self.assertFalse(self.chronosProcess.isRunning)

    def tearDown(self):
        self.patcher.stop()

    @mock.patch('Chronos.Gateway.Thread')
    def StartProcess(self, mockThread):
        self.chronosProcess.Startup()
        return mockThread.return_value

    def test_StartupShouldStartProcessingThreadAndSetRunningFlag(self):
        self.assertFalse(self.chronosProcess.isRunning)
        mockThread = self.StartProcess()

        self.assertTrue(self.chronosProcess.isRunning)
        mockThread.start.assert_called_once_with()

    def test_DuplicateStartupShouldNotStartMultipleThreads(self):
        self.assertFalse(self.chronosProcess.isRunning)
        self.StartProcess()
        mockThread = self.StartProcess()

        self.assertTrue(self.chronosProcess.isRunning)
        self.assertEqual(0, mockThread.start.call_count)

    def test_ShutdownWithoutStartupShouldDoNothing(self):
        self.chronosProcess.Shutdown()
        self.assertFalse(self.chronosProcess.isRunning)

    def test_ShutdownAfterStartupShouldCleanupInstanceAndSetRunningFlag(self):
        mockThread = self.StartProcess()
        self.assertTrue(self.chronosProcess.isRunning)
        self.chronosProcess.Shutdown()

        self.assertFalse(self.chronosProcess.isRunning)
        mockThread.join.assert_called_once_with()
        self.mockCoreProvider.Dispose.assert_called_once_with()

    def test_SendEventWhileRunningShouldAddEventToQueue(self):
        self.StartProcess()
        self.chronosProcess.SendEvent('some event')

        self.assertEqual(1, self.chronosProcess.queue.qsize())
        self.assertEqual('some event', self.chronosProcess.queue.get())

    def test_SendEventWhileNotRunningShouldRaiseChronosGatewayException(self):
        self.assertRaises(ChronosGatewayException, self.chronosProcess.SendEvent, 'anything')

    def test_EventLoopWhileNotRunningShouldRaiseChronosGatewayException(self):
        self.assertRaises(ChronosGatewayException, self.chronosProcess.EventLoop)

    def test_EventLoopShouldProcessRemainingQueueOnceShutdown(self):
        self.StartProcess()
        queryItem = ChronosEventQueueItem(1, 'type', 'proto', 2, 3, 'recv', 'tag')
        for i in xrange(0, 3):
            self.chronosProcess.SendEvent(queryItem)
        self.chronosProcess.ProcessEvent = mock.MagicMock()
        type(self.chronosProcess).isRunning = mock.PropertyMock(side_effect=itertools.chain([True], itertools.repeat(False)))
        self.chronosProcess.EventLoop()

        self.assertEqual(self.chronosProcess.ProcessEvent.call_args_list, [mock.call(queryItem), mock.call(queryItem), mock.call(queryItem)])

    def test_ProcessEventWithNoAggregateIdShouldCreateNewAggregate(self):
        self.StartProcess()
        mockAggregate = MockAggregate(aggregateId=0, version=1, lock=mock.MagicMock())
        self.mockAggregateRepository.Create.return_value = mockAggregate
        mockEventQueueItem = ChronosEventQueueItem(aggregateId=0, eventType='Chronos.Test.TestEvent',
                                                   eventProto=None, requestId=123, senderId=1, receivedTimestamp=9999, tag=None)
        self.chronosProcess.SendEvent(mockEventQueueItem)
        type(self.chronosProcess).isRunning = mock.PropertyMock(side_effect=itertools.chain([True], itertools.repeat(False)))
        self.chronosProcess.EventLoop()

        self.mockEventProcessor.ProcessIndexDivergence.assert_called_once_with(0)
        self.mockEventProcessor.EnqueueForPersistence.assert_called_once_with(self.mockBufferItem)
        self.assertEqual(0, self.mockAggregateRepository.Rollback.call_count)
        self.assertEqual(0, self.mockEventProcessor.Rollback.call_count)

    @parameterized.expand([
        ('Diverged', MockDivergedAggregate(aggregateId=1, version=2, lock=mock.MagicMock()), 1),
        ('NonDiverged', MockNonDivergedAggregate(aggregateId=1, version=2, lock=mock.MagicMock()), 0)
    ])
    def test_ProcessEventWithAggregateIdShouldUpsertAggregateSnapshotBasedOnDivergence(self, name, mockAggregate, reindexCallCount):
        self.StartProcess()
        self.mockAggregateRepository.Get.return_value = mockAggregate
        mockEventQueueItem = ChronosEventQueueItem(aggregateId=1, eventType='Chronos.Test.TestEvent',
                                                   eventProto=None, requestId=123, senderId=1, receivedTimestamp=9999, tag=None)
        self.chronosProcess.SendEvent(mockEventQueueItem)
        type(self.chronosProcess).isRunning = mock.PropertyMock(side_effect=itertools.chain([True], itertools.repeat(False)))
        self.chronosProcess.EventLoop()

        self.assertEqual(reindexCallCount, self.mockEventProcessor.ProcessIndexDivergence.call_count)

    @parameterized.expand([
        ('NewAggregateWithoutTag', MockAggregate(aggregateId=0, version=1, lock=mock.MagicMock()), None),
        ('NewAggregateWithTag', MockAggregate(aggregateId=0, version=1, lock=mock.MagicMock()), 'some tag'),
        ('ExistingAggregateWithoutTag', MockNonDivergedAggregate(aggregateId=3, version=10, lock=mock.MagicMock()), None),
        ('ExistingAggregateWithTag', MockNonDivergedAggregate(aggregateId=4, version=100, lock=mock.MagicMock()), 'another tag')
    ])
    def test_ProcessEventShouldCreateTagBasedOnEventQueueItemTagInformation(self, name, mockAggregate, tag):
        self.StartProcess()
        self.mockAggregateRepository.Get.return_value = mockAggregate
        self.mockAggregateRepository.Create.return_value = mockAggregate
        mockEventQueueItem = ChronosEventQueueItem(aggregateId=mockAggregate.aggregateId, eventType='Chronos.Test.TestEvent',
                                                   eventProto=None, requestId=123, senderId=1, receivedTimestamp=9999, tag=tag)
        self.chronosProcess.SendEvent(mockEventQueueItem)
        type(self.chronosProcess).isRunning = mock.PropertyMock(side_effect=itertools.chain([True], itertools.repeat(False)))
        self.chronosProcess.EventLoop()

        expectedCalls = 0 if tag is None else 1
        self.assertEqual(expectedCalls, self.mockEventProcessor.ProcessTag.call_count)

    def test_ProcessEventShouldProcessAllEventsInTransaction(self):
        self.StartProcess()
        self.mockAggregateRepository.Get.return_value = MockAggregate(aggregateId=1, version=1, lock=mock.MagicMock())
        self.mockAggregateRepository.Create.return_value = MockAggregate(aggregateId=1, version=1, lock=mock.MagicMock())
        event = ChronosEventQueueItem(aggregateId=1, eventType='Chronos.Test.TestEvent',
                                      eventProto=None, requestId=123, senderId=1, receivedTimestamp=9999, tag=None)
        transactionItem = ChronosEventQueueTransaction([event, event, event])

        self.chronosProcess.SendEvent(transactionItem)
        type(self.chronosProcess).isRunning = mock.PropertyMock(side_effect=itertools.chain([True], itertools.repeat(False)))
        self.chronosProcess.EventLoop()

        self.assertEqual(3, self.mockEventProcessor.EnqueueForPersistence.call_count)

    def test_QueryWhileNotRunningShouldRaiseChronosGatewayException(self):
        self.assertRaises(ChronosGatewayException, self.chronosProcess.Query, 'anything')

    def test_QueryGetAllShouldReturnAllAggregatesInRepository(self):
        self.StartProcess()
        self.mockAggregateRepository.GetAll.return_value = 'all aggregates'

        self.assertEqual('all aggregates', self.chronosProcess.Query(ChronosQueryItem()))

    def test_QueryGetByIdShouldReturnAggregateForId(self):
        self.StartProcess()
        self.mockAggregateRepository.Get.return_value = 'aggregate'

        aggregate, = self.chronosProcess.Query(ChronosQueryItem(aggregateId=123))
        self.assertEqual('aggregate', aggregate)

    def test_QueryGetByIndex(self):
        self.StartProcess()
        self.mockAggregateRepository.GetFromIndex.return_value = ['first', 'second']

        aggregates = self.chronosProcess.Query(ChronosQueryItem(indexKeys={'one': 1, 'two': 2}))
        self.assertEqual(['first', 'second'], aggregates)

    def test_TryVerifyEventPersistenceCheckpointWhileNotRunning(self):
        self.chronosProcess.isRunning = False
        self.chronosProcess._tryVerifyEventPersistenceCheckpoint()
        self.assertFalse(self.mockEventProcessor.TryVerifyEventPersistenceCheckpoint.called)

    def test_TryVerifyEventPersistenceCheckpoint(self):
        self.StartProcess()
        self.chronosProcess._tryVerifyEventPersistenceCheckpoint()
        self.mockEventProcessor.TryVerifyEventPersistenceCheckpoint.assert_called_once_with()

    # @mock.patch('Chronos.Gateway.time')
    # def test_TryVerifyEventPersistenceCheckpoint(self, time):
    #     self.StartProcess()
    #     type(self.chronosProcess).isRunning = mock.PropertyMock(side_effect=itertools.chain([True], itertools.repeat(False)))
    #     self.mockEventProcessor.TryVerifyEventPersistenceCheckpoint.side_effect = ChronosFailedEventPersistenceException()
    #     self.chronosProcess._tryVerifyEventPersistenceCheckpoint()
    #     time.sleep.assert_called_once_with(1)
    #     self.mockEventProcessor.TryVerifyEventPersistenceCheckpoint.assert_called_once_with()

    def test_Rollback(self):
        self.StartProcess()
        self.chronosProcess.Rollback()
        self.mockEventProcessor.Rollback.assert_called_once_with()
        self.mockAggregateRepository.Rollback.assert_called_once_with()


class ChronosRegistryItemTest(unittest.TestCase):
    def setUp(self):
        multiprocessingMock = mock.MagicMock()
        multiprocessingManagersMock = multiprocessingMock.managers
        multiprocessingManagersMock.BaseManager = mock.MagicMock
        multiprocessingManagersMock.BaseManager.register = mock.MagicMock()
        self.patcher = mock.patch.dict('sys.modules',
            {'Chronos.EventLogger': mock.MagicMock(),
             'Chronos.Chronos_pb2': mock.MagicMock(),
             'Chronos.Core': mock.MagicMock(),
             'multiprocessing': multiprocessingMock,
             'multiprocessing.managers': multiprocessingManagersMock})
        self.patcher.start()

        global ChronosGatewayException
        from Chronos.Gateway import ChronosRegistryItem, ChronosGatewayException
        self.mockManager = mock.MagicMock()
        self.mockProcess = mock.MagicMock()
        self.chronosRegistryItem = ChronosRegistryItem()

    def tearDown(self):
        self.patcher.stop()

    def test_Startup(self):
        self.chronosRegistryItem.Startup(self.mockManager, self.mockProcess, None)

        self.mockProcess.Startup.assert_called_once_with()
        self.assertFalse(self.mockManager.start.called)

    def test_Shutdown(self):
        self.chronosRegistryItem.Startup(self.mockManager, self.mockProcess, None)
        self.chronosRegistryItem.Shutdown()

        self.mockProcess.Shutdown.assert_called_once_with()
        self.mockManager.Shutdown.assert_called_once_with()

    def test_ShutdownNotStarted(self):
        self.chronosRegistryItem.Shutdown()

        self.assertFalse(self.chronosRegistryItem.isRunning)


class ChronosGatewayTest(unittest.TestCase):
    def setUp(self):
        multiprocessingMock = mock.MagicMock()
        multiprocessingManagersMock = multiprocessingMock.managers
        multiprocessingManagersMock.BaseManager = mock.MagicMock
        multiprocessingManagersMock.BaseManager.register = mock.MagicMock()
        self.mockLogicCompiler = mock.MagicMock()
        logicCompilerCtor = mock.MagicMock(return_value=self.mockLogicCompiler)
        self.patcher = mock.patch.dict('sys.modules',
            {'Chronos.Core': mock.MagicMock(AggregateLogicCompiler=logicCompilerCtor),
             'Chronos.EventLogger': mock.MagicMock(),
             'Chronos.Profiling': mock.MagicMock(),
             'Chronos.Chronos_pb2': mock.MagicMock(),
             'multiprocessing': multiprocessingMock,
             'multiprocessing.managers': multiprocessingManagersMock,
             'pickle': mock.MagicMock()})
        self.patcher.start()
        self.patchOpen = mock.patch('__builtin__.open', mock.MagicMock())
        self.patchOpen.start()
        self.patchStatsLogger = mock.patch('Chronos.Gateway.StatisticsLogger', mock.MagicMock())
        self.patchStatsLogger.start()

        global ChronosGatewayException
        from Chronos.Gateway import ChronosGateway, ChronosGatewayException
        mockCrossAggregateAccess = mock.MagicMock()
        mockCrossAggregateAccess.GetOrCreateMetadata.return_value = 1
        self.mockSyncManager = mock.MagicMock()
        self.mockCoreProviderGenerator = mock.MagicMock()
        self.mockGatewayStore = mock.MagicMock(crossAggregateAccess=mockCrossAggregateAccess)
        self.chronosGateway = ChronosGateway(self.mockSyncManager, self.mockCoreProviderGenerator, self.mockGatewayStore)

    def tearDown(self):
        self.chronosGateway.Shutdown()
        self.patcher.stop()
        self.patchOpen.stop()
        self.patchStatsLogger.stop()

    def test_CreateRegistrationResponseWithNoArgumentsShouldRaiseChronosGatewayException(self):
        self.assertRaises(ChronosGatewayException, self.chronosGateway.CreateRegistrationResponse)

    # @mock.patch('Chronos.Gateway.ChronosRegistrationResponse')
    # def test_CreateRegistrationResponseWithIndexedAttributesShouldSortAttributesAndReturnSuccess(self, mockResponse):
    #     response = mockResponse.return_value
    #     response.indexedAttributes = []
    #     self.chronosGateway.CreateRegistrationResponse(indexedAttributes=[3, 1, 2], caseInsensitiveAttributes=[])

        # self.assertEqual(response.indexedAttributes, [1, 2, 3])
        # self.assertEqual(response.responseCode, mockResponse.SUCCESS)

    @mock.patch('Chronos.Gateway.ChronosRegistrationResponse')
    def test_CreateRegistrationResponseWithErrorMessageShouldSetMessageAndReturnFailure(self, mockResponse):
        response = mockResponse.return_value
        self.chronosGateway.CreateRegistrationResponse(errorMessage='this is just a test')

        self.assertEqual(response.responseCode, mockResponse.FAILURE)
        self.assertEqual(response.responseMessage, 'this is just a test')

    def test_RegisterNewItem(self):
        mockLogic = mock.MagicMock()
        mockRequest = mock.MagicMock(aggregateName='TestName', aggregateLogic=mockLogic)
        self.mockLogicCompiler.Compile.return_value = (1, mock.MagicMock(__name__='TestName',
                                                                         AggregateClass=mock.MagicMock(__name__='TestName', IndexedAttributes=[])))
        self.chronosGateway.Register(mockRequest)

        self.mockLogicCompiler.Compile.assert_called_once_with(1, 'TestName', mockLogic)
        self.assertEqual(self.chronosGateway.registry['TestName'].module.__name__, 'TestName')

    def test_RegisterExistingItem(self):
        mockRegistryItem = mock.MagicMock()
        mockLogic = mock.MagicMock()
        mockRequest = mock.MagicMock(aggregateName='TestName', aggregateLogic=mockLogic)
        self.mockLogicCompiler.Compile.return_value = (1, mock.MagicMock(__name__='TestName',
                                                                         AggregateClass=mock.MagicMock(__name__='TestName', IndexedAttributes=[])))
        self.chronosGateway.registry['TestName'] = mockRegistryItem
        self.chronosGateway.Register(mockRequest)

        mockRegistryItem.Shutdown.assert_called_once_with()
        self.mockLogicCompiler.Compile.assert_called_once_with(1, 'TestName', mockLogic)

    def test_AddRegistryItemForNewItem(self):
        mockModule = mock.MagicMock()
        mockModule.AggregateClass.__name__ = 'TestAggregate'
        mockModule.__name__ = 'Test'
        mockRegistryItem = mock.MagicMock()
        self.chronosGateway.registry['Test'] = mockRegistryItem
        self.chronosGateway.AddRegistryItem(mock.MagicMock(), mockModule)

        self.assertEqual(mockRegistryItem.Startup.call_count, 1)

    def test_GetRegistryItem(self):
        mockRegistryItem = mock.MagicMock()
        self.chronosGateway.registry['Test'] = mockRegistryItem
        item = self.chronosGateway.GetRegistryItem('Test')

        self.assertEqual(item, mockRegistryItem)

    def test_GetRegistryItemMissingFailure(self):
        self.assertRaises(ChronosGatewayException, self.chronosGateway.GetRegistryItem, 'anything')

    def test_GetRegistryItemNotRunningFailure(self):
        pass

    def test_Unregister(self):
        mockRegistryItem = mock.MagicMock()
        self.chronosGateway.registry['Test'] = mockRegistryItem
        self.chronosGateway.SaveRegistry = mock.MagicMock()
        self.chronosGateway.Unregister('Test')

        mockRegistryItem.Shutdown.assert_called_once_with()
        self.chronosGateway.SaveRegistry.assert_called_once_with()
        self.assertTrue('Test' in self.chronosGateway.registry)

    def test_UnregisterForMissingItem(self):
        self.chronosGateway.SaveRegistry = mock.MagicMock()
        self.chronosGateway.Unregister('Missing')

        self.assertFalse(self.chronosGateway.SaveRegistry.called)
        self.assertFalse('Missing' in self.chronosGateway.registry)

    def test_UnregisterFailure(self):
        mockRegistryItem = mock.MagicMock()
        mockRegistryItem.Shutdown.side_effect = Exception()
        self.chronosGateway.SaveRegistry = mock.MagicMock()
        self.chronosGateway.registry['Test'] = mockRegistryItem

        self.assertRaises(ChronosGatewayException, self.chronosGateway.Unregister, 'Test')
        self.chronosGateway.SaveRegistry.assert_called_once_with()
        self.assertTrue('Test' in self.chronosGateway.registry)

    @mock.patch('Chronos.Gateway.ChronosEventQueueItem')
    def test_RouteEvent(self, mockEventQueueItem):
        mockRegistryItem = mock.MagicMock()
        self.chronosGateway.registry['Test'] = mockRegistryItem
        mockRequest = mock.MagicMock(eventType='Test.Event', aggregateId=123, eventProto='proto string')
        self.chronosGateway.RouteEvent(mockRequest)

        mockRegistryItem.process.SendEvent.assert_called_once_with(mockEventQueueItem.return_value)

    @mock.patch('Chronos.Gateway.ChronosEventQueueItem')
    @mock.patch('time.time')
    def test_RouteEventMultiple(self, mockTime, mockEventQueueItem):
        mockTime.side_effect = [1, 2]
        mockEventQueueItem.side_effect = ['first', 'second']
        mockTestRegistryItem = mock.MagicMock()
        mockOtherRegistryItem = mock.MagicMock()
        self.chronosGateway.registry['Test'] = mockTestRegistryItem
        self.chronosGateway.registry['Other'] = mockOtherRegistryItem
        mockTestRequest = mock.MagicMock(eventType='Test.Event', senderId=4, aggregateId=123, eventProto='proto string')
        mockOtherRequest = mock.MagicMock(eventType='Other.AnEvent', senderId=5, aggregateId=456, eventProto='proto string 2')
        self.chronosGateway.RouteEvent(mockTestRequest)
        self.chronosGateway.RouteEvent(mockOtherRequest)

        self.assertEqual(mockEventQueueItem.call_args_list,
            [mock.call(123, 'Event', 'proto string', 1, 4, 1e9, None),
             mock.call(456, 'AnEvent', 'proto string 2', 2, 5, 2e9, None)])
        mockTestRegistryItem.process.SendEvent.assert_called_once_with('first')
        mockOtherRegistryItem.process.SendEvent.assert_called_once_with('second')

    def test_RouteEventFailure(self):
        mockRequest = mock.MagicMock(eventType='Test.Event')
        self.assertRaises(ChronosGatewayException, self.chronosGateway.RouteEvent, mockRequest)

    @mock.patch('Chronos.Gateway.ChronosQueryItem')
    def test_GetAll(self, mockQueryItem):
        mockRequest = mock.MagicMock(aggregateName='foobar')
        self.chronosGateway._performQuery = mock.MagicMock()
        response = self.chronosGateway.GetAll(mockRequest)

        mockQueryItem.assert_called_once_with()
        self.chronosGateway._performQuery.assert_called_once_with('foobar', mockQueryItem.return_value)
        self.assertEqual(response, self.chronosGateway._performQuery.return_value)

    @mock.patch('Chronos.Gateway.ChronosQueryItem')
    def test_GetById(self, mockQueryItem):
        mockRequest = mock.MagicMock(aggregateName='foobar', aggregateId=123)
        self.chronosGateway._performQuery = mock.MagicMock()
        response = self.chronosGateway.GetById(mockRequest)

        mockQueryItem.assert_called_once_with(aggregateId=123)
        self.chronosGateway._performQuery.assert_called_once_with('foobar', mockQueryItem.return_value)
        self.assertEqual(response, self.chronosGateway._performQuery.return_value)

    @mock.patch('Chronos.Gateway.ChronosQueryItem')
    def test_GetByIndex(self, mockQueryItem):
        mockRequest = mock.MagicMock(aggregateName='foobar', indexKeys=['one', 'two'])
        self.chronosGateway._performQuery = mock.MagicMock()
        response = self.chronosGateway.GetByIndex(mockRequest)

        mockQueryItem.assert_called_once_with(indexKeys={'one': 'two'})
        self.chronosGateway._performQuery.assert_called_once_with('foobar', mockQueryItem.return_value)
        self.assertEqual(response, self.chronosGateway._performQuery.return_value)

    def test_GetByIndexMalformed(self):
        mockRequest = mock.MagicMock(aggregateName='foobar', indexKeys=['one'])
        self.assertRaises(ChronosGatewayException, self.chronosGateway.GetByIndex, mockRequest)

    def test_GetByTag(self):
        mockRegistryItem = mock.MagicMock()
        mockRequest = mock.MagicMock(aggregateName='taggregate', tag='tagtest', aggregateId=1)
        self.chronosGateway.GetRegistryItem = mock.MagicMock(return_value=mockRegistryItem)
        self.chronosGateway.GetByTag(mockRequest)

        self.chronosGateway.GetRegistryItem.assert_called_once_with('taggregate')
        mockRegistryItem.process.GetByTag.assert_called_once_with('tagtest', 1)

    def test_Shutdown(self):
        mockRegistryItem = mock.MagicMock()
        mockRegistryItem2 = mock.MagicMock()
        self.chronosGateway.registry = {1: mockRegistryItem, 2: mockRegistryItem2}
        self.chronosGateway.Shutdown()

        mockRegistryItem.Shutdown.assert_called_once_with()
        mockRegistryItem2.Shutdown.assert_called_once_with()
        self.assertEqual(len(self.chronosGateway.registry), 2)

    @mock.patch('Chronos.Gateway.ChronosRegistrationResponse')
    def test_CheckStatusForUnknownAggregateShouldReturnFailure(self, mockResponse):
        response = mockResponse.return_value
        self.assertEqual(self.chronosGateway.CheckStatus('missing'), response.SerializeToString.return_value)
        self.assertEqual(response.responseCode, mockResponse.FAILURE)
        self.assertEqual(response.responseMessage, 'Aggregate missing not found in registry')
