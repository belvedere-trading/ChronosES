#pylint: skip-file
import mock
import unittest
from mock import patch

class FakeEvent(object):
    def ParseFromString(self, proto):
        pass

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return not self.__eq__(other)

class Foo(FakeEvent):
    pass

class Bar(FakeEvent):
    pass

class ChronosClientTest(unittest.TestCase):
    def setUp(self):
        self.mockPkgResources = mock.MagicMock()
        self.patcher = mock.patch.dict('sys.modules',
            {'Ice': mock.MagicMock(),
             'pkg_resources': self.mockPkgResources,
             'Chronos.EventLogger': mock.MagicMock(),
             'Chronos.Processor': mock.MagicMock(),
             'Chronos.Chronos_pb2': mock.MagicMock(),
             'Chronos.Infrastructure': mock.MagicMock()})
        self.patcher.start()

        global ChronosClient, EventResponse, ChronosTransactionRequest, ChronosRequestWithTag, ChronosClientException, TagNotFound
        from Chronos.Client import ChronosClient, EventResponse, ChronosTransactionRequest, ChronosRequestWithTag, ChronosClientException, TagNotFound
        self.mockProtoClass = mock.MagicMock()
        self.mockProtoClass.__name__ = 'MockProto'
        self.mockRedisController = mock.MagicMock()
        self.mockSubscriptionManager = mock.MagicMock()
        self.mockInfrastructureProvider = mock.MagicMock()
        self.chronosClient = ChronosClient(self.mockProtoClass, infrastructureProvider=self.mockInfrastructureProvider, redisController=self.mockRedisController)
        self.chronosClient.subscriptionManager = self.mockSubscriptionManager

    def tearDown(self):
        self.patcher.stop()

    @mock.patch('Chronos.Client.SubscriptionManager')
    def test_Construction(self, mockSubscriptionManager):
        self.mockInfrastructureProvider = mock.MagicMock()
        chronosClient = ChronosClient(mock.MagicMock(__name__='MockProto'), infrastructureProvider=self.mockInfrastructureProvider,
                                                     redisController=self.mockRedisController)

        self.assertEqual(chronosClient.aggregateName, 'MockProto')
        mockSubscriptionManager.return_value.SubscribeForUnifiedUpdates.assert_called_once_with()
        self.assertEqual(chronosClient.serviceProxyManager, self.mockInfrastructureProvider.GetConfigurablePlugin.return_value)
        chronosClient.serviceProxyManager.Connect.assert_called_once_with()

    def test_Connect(self):
        self.chronosClient.serviceProxyManager.reset_mock()
        self.chronosClient.Connect()
        self.chronosClient.serviceProxyManager.Connect.assert_called_once_with()

    def test_Dispose(self):
        mockServiceProxyManager = self.chronosClient.serviceProxyManager
        self.chronosClient.Dispose()
        self.mockSubscriptionManager.UnsubscribeForUnifiedUpdates.assert_called_once_with()
        mockServiceProxyManager.Dispose.assert_called_once_with()

    def test_Disconnect(self):
        self.chronosClient.Disconnect()
        self.chronosClient.serviceProxyManager.Disconnect.assert_called_once_with()

    @mock.patch('Chronos.Client.ChronosRegistrationResponse')
    def test_processRegistrationResponseSuccess(self, mockRegistrationResponse):
        response = mockRegistrationResponse.return_value
        response.responseCode = mockRegistrationResponse.SUCCESS
        response.indexedAttributes = ['one', 'two']
        self.chronosClient._processRawRegistrationReponse(None)

        self.mockSubscriptionManager.Resubscribe.assert_called_once_with(['one', 'two'])

    @mock.patch('Chronos.Client.ChronosRegistrationResponse')
    def test_processRegistrationResponseFailure(self, mockRegistrationResponse):
        self.chronosClient._processRawRegistrationReponse(None)

        self.assertEqual(self.mockSubscriptionManager.Resubscribe.call_count, 0)

    @mock.patch('Chronos.Client.ChronosManagementNotification')
    def test_managementCallbackRegistered(self, mockManagementNotification):
        notification = mockManagementNotification.return_value
        notification.notificationType = mockManagementNotification.REGISTERED
        notification.indexedAttributes = ['something']
        self.chronosClient._managementCallback({'data': None})

        self.mockSubscriptionManager.Resubscribe.assert_called_once_with(['something'])

    @mock.patch('Chronos.Client.ChronosManagementNotification')
    def test_managementCallbackNonRegistered(self, mockManagementNotification):
        self.chronosClient._managementCallback({'data': None})

        self.assertEqual(self.mockSubscriptionManager.Resubscribe.call_count, 0)

    def test_TypeOf(self):
        mockInstance = mock.MagicMock()
        mockInstance.__class__.__module__ = 'Test.Event_pb2'
        parsed = self.chronosClient.TypeOf(mockInstance.__class__)

        self.assertEqual(parsed, 'MockProto.MagicMock')

    def test_ParseAggregate(self):
        mockAggregateProto = mock.MagicMock(aggregateId=123, version=23)
        aggregate = self.chronosClient.ParseAggregate(mockAggregateProto)

        self.mockProtoClass.assert_called_once_with()
        self.assertEqual(self.mockProtoClass.return_value.ParseFromString.call_count, 1)
        self.assertEqual(aggregate.aggregateId, 123)
        self.assertEqual(aggregate.version, 23)

    def test_Register(self):
        self.mockPkgResources.resource_string.side_effect = ['python', 'proto']
        self.chronosClient.Register()

        self.assertEqual(self.mockPkgResources.resource_string.call_args_list,
            [mock.call('Chronos', 'ChronosScripts/MockProto.py'), mock.call('Chronos', 'ChronosScripts/MockProto.proto')])
        self.assertEqual(self.chronosClient.client.RegisterAggregate.call_count, 1)

    def test_Unregister(self):
        self.chronosClient.Unregister()

        self.chronosClient.client.UnregisterAggregate.assert_called_once_with('MockProto')

    def test_RaiseEvent(self):
        mockEvent = mock.MagicMock()
        mockEvent.__class__.__module__ = 'Test.Event_pb2'
        mockEvent.SerializeToString.return_value = 'proto string'
        self.chronosClient.client.ProcessEvent.return_value = 999
        self.chronosClient.RaiseEvent(mockEvent)

    def test_Subscribe(self):
        self.chronosClient.Subscribe()

        self.assertEqual(self.mockSubscriptionManager.Subscribe.call_count, 1)

    def test_Unsubscribe(self):
        self.chronosClient.Unsubscribe()

        self.assertEqual(self.mockSubscriptionManager.Unsubscribe.call_count, 1)

    def test_GetAllAggregates(self):
        self.chronosClient.GetAllAggregates()

        self.assertEquals(self.chronosClient.client.GetAll.call_count, 1)

    def test_GetAggregateById(self):
        self.chronosClient._getQueryResponse = mock.MagicMock(return_value=[1])
        self.chronosClient.GetAggregateById(mock.MagicMock())

        self.assertEquals(self.chronosClient.client.GetById.call_count, 1)

    def test_GetAggregatesByIndex(self):
        self.chronosClient.GetAggregatesByIndex(index='anything')

        self.assertEquals(self.chronosClient.client.GetByIndex.call_count, 1)

    def test_ParseEventResponse(self):
        eventProto = mock.MagicMock
        eventProto.type = 'MockProto.Foo'
        eventProto.proto = mock.MagicMock()
        eventProto.version = 1
        eventProto.logicVersion = 2
        eventProto.receivedTimestamp = 3
        eventProto.processedTimestamp = 4
        self.chronosClient.EventTypes = [Foo, Bar]
        response = self.chronosClient.ParseEventResponse(eventProto)

        self.assertEqual(response.event, Foo())
        self.assertEqual(response.version, 1)
        self.assertEqual(response.logicVersion, 2)
        self.assertEqual(response.receivedTimestamp, 3)
        self.assertEqual(response.processedTimestamp, 4)

    def test_ParseBadEvent(self):
        badEventProto = mock.MagicMock()
        badEventProto.type = 'MockProto.NotFooBar'
        self.assertRaises(ValueError, self.chronosClient.ParseEventResponse, badEventProto)

    def test_RaiseEventWithTag(self):
        mockEvent = mock.MagicMock()
        self.chronosClient.client.ProcessEventWithTag.return_value = 1

        self.assertEqual(self.chronosClient.RaiseEventWithTag('tag', mockEvent), 1)

    def test_RaiseBadEventWithTag(self):
        mockEvent = mock.MagicMock()
        mockEvent.SerializeToString.side_effect = ValueError()

        self.assertRaises(ChronosClientException, self.chronosClient.RaiseEventWithTag, 'tag', mockEvent)

    def test_RaiseTransaction(self):
        mockEvent = mock.MagicMock()
        self.chronosClient.client.ProcessTransaction.return_value = 1

        self.assertEqual(self.chronosClient.RaiseTransaction([mockEvent]), 1)

    def test_RaiseBadTransaction(self):
        mockEvent = mock.MagicMock()
        mockEvent.event.SerializeToString.side_effect = ValueError()

        self.assertRaises(ChronosClientException, self.chronosClient.RaiseTransaction, [mockEvent])

    @patch('Chronos.Client.ChronosQueryResponse')
    def test_GetAggregateByTag(self, queryResponse):
        queryResponse.return_value.responseCode = queryResponse.TAG_NOT_FOUND
        self.assertRaises(TagNotFound, self.chronosClient.GetAggregateByTag, 0, 'no_tag')

    @patch('Chronos.Client.ChronosQueryResponse')
    @patch('Chronos.Client.ChronosQueryByTagRequest')
    def test_GetAggregateByTag(self, queryByTagRequest, queryResponse):
        queryResponse.return_value.aggregates = [1]
        queryByTagRequest.return_value.SerializeToString.return_value = 2
        self.chronosClient.ParseAggregate = mock.MagicMock()
        self.chronosClient.client.GetByTag = mock.MagicMock()

        self.chronosClient.GetAggregateByTag(1, 'tag')
        self.chronosClient.ParseAggregate.assert_called_once_with(1)
        self.chronosClient.client.GetByTag.assert_called_once_with(2)
