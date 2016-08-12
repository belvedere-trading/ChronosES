#pylint: skip-file
import mock
import unittest

class ChronosClientTest(unittest.TestCase):
    def setUp(self):
        self.mockPkgResources = mock.MagicMock()
        self.patcher = mock.patch.dict('sys.modules',
            {'Ice': mock.MagicMock(),
             'pkg_resources': self.mockPkgResources,
             'Chronos.EventLogger': mock.MagicMock(),
             'Chronos.Chronos_pb2': mock.MagicMock(),
             'Chronos.Infrastructure': mock.MagicMock()})
        self.patcher.start()

        global ChronosClient
        from Chronos.Client import ChronosClient
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
        chronosClient = ChronosClient(mock.MagicMock(__name__='MockProto'), infrastructureProvider=self.mockInfrastructureProvider, redisController=self.mockRedisController)

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
