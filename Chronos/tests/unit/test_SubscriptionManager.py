import mock
import unittest
from nose_parameterized import parameterized


class DummyAggregate(object):
    def __init__(self):
        pass


class SubscriptionManagerTests(unittest.TestCase):
    def setUp(self):
        googleMock = mock.MagicMock()
        protobufMock = googleMock.protobuf
        protobufMock.message = mock.MagicMock()
        protobufMock.message.Message = mock.MagicMock
        self.patcher = mock.patch.dict('sys.modules', {
            'google': googleMock,
            'google.protobuf': protobufMock,
            'psycopg2': mock.MagicMock(),
            'psycopg2.extras': mock.MagicMock(),
            'redis': mock.MagicMock(),
            'Chronos': mock.MagicMock(),
            'Chronos.EventLogger': mock.MagicMock(),
            'Chronos.Processor': mock.MagicMock(),
            'Chronos.Map': mock.MagicMock(),
            'Chronos.Chronos_pb2': mock.MagicMock(),
        })
        self.patcher.start()

        self.redisConnection = mock.MagicMock()
        self.mockSuccessCallback = mock.MagicMock()
        self.mockFailureCallback = mock.MagicMock()
        self.mockManagementCallback = mock.MagicMock()
        infrastructureProvider = mock.MagicMock()

        from Chronos.DefaultImplementations import RedisNotifier, RedisEventKeys
        self.notifier = RedisNotifier(infrastructureProvider, DummyAggregate, self.redisConnection)

        global ChronosClientException
        from Chronos.Client import SubscriptionManager, ChronosClientException
        self.subscriptionManager = SubscriptionManager(self.notifier, DummyAggregate,
                                                       self.mockSuccessCallback, self.mockFailureCallback,
                                                       self.mockManagementCallback)
        self.notifier.Subscribe = mock.MagicMock()
        self.notifier.Unsubscribe = mock.MagicMock()
        self.notifier.transaction = mock.MagicMock()
        self.dummyNotification = mock.MagicMock()
        self.dummyNotification.SerializeToString.return_value = 'serialized'
        self.mockCoreProvider = mock.MagicMock()
        mockCoreProviderGenerator = mock.MagicMock(return_value=self.mockCoreProvider)
        self.mockCoreProvider.GetNotifier.return_value = self.notifier

    def tearDown(self):
        self.patcher.stop()

    @staticmethod
    def getIndexedAttributes(attributes):
        indexedAttributes = []
        for attr in attributes:
            mockIndexedAttribute = mock.MagicMock()
            mockIndexedAttribute.isCaseInsensitive = False
            mockIndexedAttribute.attributeName = attr
            indexedAttributes.append(mockIndexedAttribute)
        return indexedAttributes

    @parameterized.expand([
        ('NoErrors', [None, None]),
        ('FirstError', [ValueError('Redis problem'), None]),
        ('SecondError', [None, KeyError('Weird')]),
        ('BothError', [KeyError('Strange'), ValueError('Uh oh')])
    ])
    def test_SubscribeForUnifiedUpdates_should_subscribe_despite_errors(self, name, sideEffects):
        self.notifier.Subscribe.side_effect = sideEffects
        self.subscriptionManager.SubscribeForUnifiedUpdates()

        self.assertEqual(self.notifier.Subscribe.call_args_list,
                         [mock.call('Failure.Chronos.DummyAggregate', self.mockFailureCallback),
                          mock.call('Management.Chronos.DummyAggregate', self.mockManagementCallback)])

    @parameterized.expand([
        ('NoErrors', [None, None]),
        ('FirstError', [ValueError('Redis problem'), None]),
        ('SecondError', [None, KeyError('Weird')]),
        ('BothError', [KeyError('Strange'), ValueError('Uh oh')])
    ])
    def test_UnsubscribeForUnifiedUpdates_should_unsubscribe_despite_errors(self, name, sideEffects):
        self.notifier.Unsubscribe.side_effect = sideEffects
        self.subscriptionManager.UnsubscribeForUnifiedUpdates()

        self.assertEqual(self.notifier.Unsubscribe.call_args_list,
                         [mock.call('Failure.Chronos.DummyAggregate'),
                          mock.call('Management.Chronos.DummyAggregate')])

    def test_Subscribe_without_setting_indexedAttributes_should_raise_ChronosClientException(self):
        try:
            self.subscriptionManager.Subscribe({})
        except ChronosClientException as ex:
            self.assertTrue('indexedAttributes not set' in str(ex))
        else:
            self.fail('Expected ChronosClientException')

    def test_Resubscribe_should_set_indexedAttributes(self):
        indexedAttributes = self.getIndexedAttributes(['firstAttr', 'secondAttr'])
        self.subscriptionManager.Resubscribe(indexedAttributes)
        attributeNames = [attribute.attributeName for attribute in self.subscriptionManager.indexedAttributes]
        self.assertEqual(attributeNames, ['firstAttr', 'secondAttr'])

    def test_Subscribe_with_no_indices_should_subscribe_wildcard_channel(self):
        indexedAttributes = self.getIndexedAttributes(['one', 'two'])
        self.subscriptionManager.Resubscribe(indexedAttributes)
        self.subscriptionManager.Subscribe({})

        self.notifier.Subscribe.assert_called_once_with('Chronos.DummyAggregate.*.*', self.mockSuccessCallback)

    def test_Subscribe_with_some_indices_should_subscribe_partial_wildcard_channel(self):
        indexedAttributes = self.getIndexedAttributes(['first', 'second', 'third'])
        self.subscriptionManager.Resubscribe(indexedAttributes)
        self.subscriptionManager.Subscribe({'first': 1, 'third': 3})

        self.notifier.Subscribe.assert_called_once_with('Chronos.DummyAggregate.1.*.3', self.mockSuccessCallback)

    def test_Subscribe_with_all_indices_should_subscribe_concrete_channel(self):
        indexedAttributes = self.getIndexedAttributes(['foo', 'bar'])
        self.subscriptionManager.Resubscribe(indexedAttributes)
        self.subscriptionManager.Subscribe({'foo': 'baz', 'bar': 'quux'})

        self.notifier.Subscribe.assert_called_once_with('Chronos.DummyAggregate.baz.quux', self.mockSuccessCallback)

    def test_duplicate_Subscribe_should_raise_ChronosClientException(self):
        indexedAttributes = self.getIndexedAttributes(['single'])
        self.subscriptionManager.Resubscribe(indexedAttributes)
        self.subscriptionManager.Subscribe({'single': '123'})
        try:
            self.subscriptionManager.Subscribe({'single': '123'})
        except ChronosClientException as ex:
            self.assertTrue('Already subscribed' in str(ex))
        else:
            self.fail('Expected ChronosClientException')

    def test_Unsubscribe_without_setting_indexedAttributes_should_raise_ChronosClientException(self):
        try:
            self.subscriptionManager.Unsubscribe({})
        except ChronosClientException as ex:
            self.assertTrue('indexedAttributes not set' in str(ex))
        else:
            self.fail('Expected ChronosClientException')

    def test_Unsubscribe_without_prior_Subscribe_should_not_fail(self):
        self.subscriptionManager.Resubscribe([])
        self.subscriptionManager.Unsubscribe({})

        self.assertEqual(self.notifier.Unsubscribe.call_count, 0)

    def test_Unsubscribe_should_unsubscribe(self):
        self.subscriptionManager.Resubscribe([])
        self.subscriptionManager.Subscribe({})
        self.subscriptionManager.Unsubscribe({})

        self.notifier.Unsubscribe.assert_called_once_with('Chronos.DummyAggregate')

    def test_UnsubscribeAll_without_subscriptions_should_do_nothing(self):
        # If Unsubscribe were ever called, we would get a ChronosClientException since we haven't set indexedAttributes
        self.subscriptionManager.UnsubscribeAll()

    def test_UnsubscribeAll_should_unsubscribe_all_subscribed_channels(self):
        # Simulate some errors just for kicks
        self.notifier.Unsubscribe.side_effect = [None, ValueError('boom'), IndexError('out of range')]
        indexedAttributes = self.getIndexedAttributes(['foo', 'baz', 'bar'])
        self.subscriptionManager.Resubscribe(indexedAttributes)
        self.subscriptionManager.Subscribe({'foo': 111})
        self.subscriptionManager.Subscribe({'baz': 1, 'bar': 2})
        self.subscriptionManager.Subscribe({'bar': 222})
        self.subscriptionManager.UnsubscribeAll()

        self.assertEqual(self.notifier.Unsubscribe.call_count, 3)
        callList = self.notifier.Unsubscribe.call_args_list
        self.assertTrue(mock.call('Chronos.DummyAggregate.111.*.*') in callList)
        self.assertTrue(mock.call('Chronos.DummyAggregate.*.1.2') in callList)
        self.assertTrue(mock.call('Chronos.DummyAggregate.*.*.222') in callList)

    def test_Resubscribe_without_existing_subscriptions_should_only_set_indexedAttributes(self):
        indexedAttributes = self.getIndexedAttributes(['anything'])
        self.subscriptionManager.Resubscribe(indexedAttributes)
        self.assertEqual(self.notifier.Subscribe.call_count, 0)
        self.assertEqual(self.notifier.Unsubscribe.call_count, 0)

    def test_Resubscribe_with_same_indexedAttributes_should_do_nothing(self):
        indexedAttributes = self.getIndexedAttributes(['attribute', 'second'])
        self.subscriptionManager.Resubscribe(indexedAttributes)
        self.subscriptionManager.Subscribe({})
        self.subscriptionManager.Resubscribe(indexedAttributes)

        self.assertEqual(self.notifier.Unsubscribe.call_count, 0)
        self.assertEqual(self.notifier.Subscribe.call_count, 1)

    def test_Resubscribe_with_new_indexedAttributes_should_migrate_all_subscriptions(self):
        indexedAttributes = self.getIndexedAttributes(['attribute', 'second'])
        self.subscriptionManager.Resubscribe(indexedAttributes)
        self.subscriptionManager.Subscribe({'attribute': 'foo'})
        self.subscriptionManager.Subscribe({'second': 2})
        self.notifier.Unsubscribe.side_effect = [ValueError('something'), None]
        self.notifier.Subscribe.side_effect = [None, KeyError('something else')]
        indexedAttributes = self.getIndexedAttributes(['attribute', 'newAttribute', 'second'])
        self.subscriptionManager.Resubscribe(indexedAttributes)

        self.assertEqual(self.notifier.Unsubscribe.call_count, 2)
        self.assertTrue(mock.call('Chronos.DummyAggregate.foo.*') in self.notifier.Unsubscribe.call_args_list)
        self.assertTrue(mock.call('Chronos.DummyAggregate.*.2') in self.notifier.Unsubscribe.call_args_list)

        self.assertEqual(self.notifier.Subscribe.call_count, 4)
        self.assertTrue(mock.call('Chronos.DummyAggregate.foo.*.*', self.mockSuccessCallback) in self.notifier.Subscribe.call_args_list)
        self.assertTrue(mock.call('Chronos.DummyAggregate.*.*.2', self.mockSuccessCallback) in self.notifier.Subscribe.call_args_list)
