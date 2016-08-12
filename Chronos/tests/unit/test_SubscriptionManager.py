import mock
import unittest
from nose_parameterized import parameterized

class SubscriptionManagerTest(unittest.TestCase):
    def setUp(self):
        self.patcher = mock.patch.dict('sys.modules', {
            'Chronos.EventLogger': mock.MagicMock(),
            'Chronos.Processor': mock.MagicMock(),
            'Chronos.Chronos_pb2': mock.MagicMock(),
        })
        self.patcher.start()

        self.mockRedisController = mock.MagicMock()
        self.mockSuccessCallback = mock.MagicMock()
        self.mockFailureCallback = mock.MagicMock()
        self.mockManagementCallback = mock.MagicMock()
        global ChronosClientException
        from Chronos.Client import SubscriptionManager, ChronosClientException
        self.subscriptionManager = SubscriptionManager(self.mockRedisController, 'TestName',
                                                       self.mockSuccessCallback, self.mockFailureCallback,
                                                       self.mockManagementCallback)

    def tearDown(self):
        self.patcher.stop()

    def test_init_should_set_subscription_channels(self):
        self.assertEqual(self.subscriptionManager.subscriptionPrefix, 'Chronos.TestName')
        self.assertEqual(self.subscriptionManager.failureChannel, 'Failure.Chronos.TestName')
        self.assertEqual(self.subscriptionManager.managementChannel, 'Management.Chronos.TestName')

    @parameterized.expand([
        ('NoErrors', [None, None]),
        ('FirstError', [ValueError('Redis problem'), None]),
        ('SecondError', [None, KeyError('Weird')]),
        ('BothError', [KeyError('Strange'), ValueError('Uh oh')])
    ])
    def test_SubscribeForUnifiedUpdates_should_subscribe_despite_errors(self, name, sideEffects):
        self.mockRedisController.SubscribeWithHashKey.side_effect = sideEffects
        self.subscriptionManager.SubscribeForUnifiedUpdates()

        self.assertEqual(self.mockRedisController.SubscribeWithHashKey.call_args_list,
                         [mock.call('Chronos.TestName', 'Failure.Chronos.TestName', self.mockFailureCallback),
                          mock.call('Chronos.TestName', 'Management.Chronos.TestName', self.mockManagementCallback)])

    @parameterized.expand([
        ('NoErrors', [None, None]),
        ('FirstError', [ValueError('Redis problem'), None]),
        ('SecondError', [None, KeyError('Weird')]),
        ('BothError', [KeyError('Strange'), ValueError('Uh oh')])
    ])
    def test_UnsubscribeForUnifiedUpdates_should_unsubscribe_despite_errors(self, name, sideEffects):
        self.mockRedisController.UnsubscribeWithHashKey.side_effect = sideEffects
        self.subscriptionManager.UnsubscribeForUnifiedUpdates()

        self.assertEqual(self.mockRedisController.UnsubscribeWithHashKey.call_args_list,
                         [mock.call('Chronos.TestName', 'Failure.Chronos.TestName'),
                          mock.call('Chronos.TestName', 'Management.Chronos.TestName')])

    def test_Subscribe_without_setting_indexedAttributes_should_raise_ChronosClientException(self):
        try:
            self.subscriptionManager.Subscribe({})
        except ChronosClientException as ex:
            self.assertTrue('indexedAttributes not set' in str(ex))
        else:
            self.fail('Expected ChronosClientException')

    def test_Resubscribe_should_set_indexedAttributes(self):
        self.subscriptionManager.Resubscribe(['firstAttr', 'secondAttr'])
        self.assertEqual(self.subscriptionManager.indexedAttributes, ['firstAttr', 'secondAttr'])

    def test_Subscribe_with_no_indices_should_subscribe_wildcard_channel(self):
        self.subscriptionManager.Resubscribe(['one', 'two'])
        self.subscriptionManager.Subscribe({})

        self.mockRedisController.SubscribeWithHashKey.assert_called_once_with('Chronos.TestName', 'Chronos.TestName.*.*', self.mockSuccessCallback)

    def test_Subscribe_with_some_indices_should_subscribe_partial_wildcard_channel(self):
        self.subscriptionManager.Resubscribe(['first', 'second', 'third'])
        self.subscriptionManager.Subscribe({'first': 1, 'third': 3})

        self.mockRedisController.SubscribeWithHashKey.assert_called_once_with('Chronos.TestName', 'Chronos.TestName.1.*.3', self.mockSuccessCallback)

    def test_Subscribe_with_all_indices_should_subscribe_concrete_channel(self):
        self.subscriptionManager.Resubscribe(['foo', 'bar'])
        self.subscriptionManager.Subscribe({'foo': 'baz', 'bar': 'quux'})

        self.mockRedisController.SubscribeWithHashKey.assert_called_once_with('Chronos.TestName', 'Chronos.TestName.baz.quux', self.mockSuccessCallback)

    def test_duplicate_Subscribe_should_raise_ChronosClientException(self):
        self.subscriptionManager.Resubscribe(['single'])
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

        self.assertEqual(self.mockRedisController.UnsubscribeWithHashKey.call_count, 0)

    def test_Unsubscribe_should_unsubscribe(self):
        self.subscriptionManager.Resubscribe([])
        self.subscriptionManager.Subscribe({})
        self.subscriptionManager.Unsubscribe({})

        self.mockRedisController.UnsubscribeWithHashKey.assert_called_once_with('Chronos.TestName', 'Chronos.TestName')

    def test_UnsubscribeAll_without_subscriptions_should_do_nothing(self):
        # If Unsubscribe were ever called, we would get a ChronosClientException since we haven't set indexedAttributes
        self.subscriptionManager.UnsubscribeAll()

    def test_UnsubscribeAll_should_unsubscribe_all_subscribed_channels(self):
        # Simulate some errors just for kicks
        self.mockRedisController.UnsubscribeWithHashKey.side_effect = [None, ValueError('boom'), IndexError('out of range')]
        self.subscriptionManager.Resubscribe(['foo', 'baz', 'bar'])
        self.subscriptionManager.Subscribe({'foo': 111})
        self.subscriptionManager.Subscribe({'baz': 1, 'bar': 2})
        self.subscriptionManager.Subscribe({'bar': 222})
        self.subscriptionManager.UnsubscribeAll()

        self.assertEqual(self.mockRedisController.UnsubscribeWithHashKey.call_count, 3)
        callList = self.mockRedisController.UnsubscribeWithHashKey.call_args_list
        self.assertTrue(mock.call('Chronos.TestName', 'Chronos.TestName.111.*.*') in callList)
        self.assertTrue(mock.call('Chronos.TestName', 'Chronos.TestName.*.1.2') in callList)
        self.assertTrue(mock.call('Chronos.TestName', 'Chronos.TestName.*.*.222') in callList)

    def test_Resubscribe_without_existing_subscriptions_should_only_set_indexedAttributes(self):
        self.subscriptionManager.Resubscribe(['anything'])
        self.assertEqual(self.mockRedisController.SubscribeWithHashKey.call_count, 0)
        self.assertEqual(self.mockRedisController.UnsubscribeWithHashKey.call_count, 0)

    def test_Resubscribe_with_same_indexedAttributes_should_do_nothing(self):
        self.subscriptionManager.Resubscribe(['attribute', 'second'])
        self.subscriptionManager.Subscribe({})
        self.subscriptionManager.Resubscribe(['attribute', 'second'])

        self.assertEqual(self.mockRedisController.UnsubscribeWithHashKey.call_count, 0)
        self.assertEqual(self.mockRedisController.SubscribeWithHashKey.call_count, 1)

    def test_Resubscribe_with_new_indexedAttributes_should_migrate_all_subscriptions(self):
        self.subscriptionManager.Resubscribe(['attribute', 'second'])
        self.subscriptionManager.Subscribe({'attribute': 'foo'})
        self.subscriptionManager.Subscribe({'second': 2})
        self.mockRedisController.UnsubscribeWithHashKey.side_effect = [ValueError('something'), None]
        self.mockRedisController.SubscribeWithHashKey.side_effect = [None, KeyError('something else')]
        self.subscriptionManager.Resubscribe(['attribute', 'newAttribute', 'second'])

        self.assertEqual(self.mockRedisController.UnsubscribeWithHashKey.call_count, 2)
        self.assertTrue(mock.call('Chronos.TestName', 'Chronos.TestName.foo.*') in self.mockRedisController.UnsubscribeWithHashKey.call_args_list)
        self.assertTrue(mock.call('Chronos.TestName', 'Chronos.TestName.*.2') in self.mockRedisController.UnsubscribeWithHashKey.call_args_list)

        self.assertEqual(self.mockRedisController.SubscribeWithHashKey.call_count, 4)
        self.assertTrue(mock.call('Chronos.TestName', 'Chronos.TestName.foo.*.*', self.mockSuccessCallback) in self.mockRedisController.SubscribeWithHashKey.call_args_list)
        self.assertTrue(mock.call('Chronos.TestName', 'Chronos.TestName.*.*.2', self.mockSuccessCallback) in self.mockRedisController.SubscribeWithHashKey.call_args_list)
