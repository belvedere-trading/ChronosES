import mock
from unittest import TestCase

class ChronosProcessSynchronizerTest(TestCase):
    def setUp(self):
        self.patcher = mock.patch.dict('sys.modules',
         'Chronos.EventLogger': mock.MagicMock(),
         'Chronos.Core': mock.MagicMock()
        })
        self.patcher.start()

        global AggregateDependency, DependenciesNotReleased, MissingDependency
        from Chronos.Dependency import ChronosProcessSynchronizer, AggregateDependency, DependenciesNotReleased, MissingDependency
        self.synchronizer = ChronosProcessSynchronizer()

    def tearDown(self):
        self.patcher.stop()

    def test_AcquireDependencies_with_missing_dependency_should_raise_MissingDependency(self):
        mockEvent = mock.MagicMock(Dependencies=[AggregateDependency('missing', None, None, None)])
        self.assertRaises(MissingDependency, self.synchronizer.AcquireDependencies, None, mockEvent)

    def test_AcquireDependencies_with_event_source_should_acquire_remote_dependencies_and_set_destination_property(self):
        mockProxy = mock.MagicMock()
        mockProxy.Acquire.return_value = 123
        mockEvent = mock.MagicMock(Dependencies=[AggregateDependency('AggregateName', 'theDestination', 'eventSource', None)],
                                   eventSource=7)
        self.synchronizer.AssignSynchronizationProxy('AggregateName', mockProxy)
        self.synchronizer.AcquireDependencies(None, mockEvent)

        self.assertEqual(mockEvent.theDestination, 123)
        mockProxy.Acquire.assert_called_once_with(7)

    def test_AcquireDependencies_with_aggregate_source_should_acquire_remote_dependencies_and_set_destination_property(self):
        mockProxy = mock.MagicMock()
        mockProxy.Acquire.return_value = 456
        mockAggregate = mock.MagicMock(aggregateSource=10)
        mockEvent = mock.MagicMock(Dependencies=[AggregateDependency('AggregateName', 'theDestination', None, 'aggregateSource')])
        self.synchronizer.AssignSynchronizationProxy('AggregateName', mockProxy)
        self.synchronizer.AcquireDependencies(mockAggregate, mockEvent)

        self.assertEqual(mockEvent.theDestination, 456)
        mockProxy.Acquire.assert_called_once_with(10)

    def test_AcquireDependencies_twice_without_releasing_should_raise_DependenciesNotReleased(self):
        mockProxy = mock.MagicMock()
        mockEvent = mock.MagicMock(Dependencies=[AggregateDependency('AggregateName', 'theDestination', 'eventSource', None)],
                                   eventSource=7)
        self.synchronizer.AssignSynchronizationProxy('AggregateName', mockProxy)
        self.synchronizer.AcquireDependencies(None, mockEvent)

        self.assertRaises(DependenciesNotReleased, self.synchronizer.AcquireDependencies, None, mockEvent)

    def test_AcquireDependencies_twice_with_releasing_should_reacquire_dependencies_and_set_destination_property(self):
        mockProxy = mock.MagicMock()
        mockProxy.Acquire.side_effect = [8, 9]
        mockEvent = mock.MagicMock(Dependencies=[AggregateDependency('AggregateName', 'theDestination', 'eventSource', None)],
                                   eventSource=7)
        self.synchronizer.AssignSynchronizationProxy('AggregateName', mockProxy)
        self.synchronizer.AcquireDependencies(None, mockEvent)
        self.synchronizer.ReleaseDependencies()
        self.synchronizer.AcquireDependencies(None, mockEvent)

        self.assertEqual(mockEvent.theDestination, 9)
        self.assertEqual(mockProxy.Acquire.call_args_list, [mock.call(7), mock.call(7)])
        mockProxy.Release.assert_called_once_with(7)
