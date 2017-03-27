import mock
from unittest import TestCase

class AggregateSynchronizationManagerTest(TestCase):
    def setUp(self):
        self.patcher = mock.patch.dict('sys.modules',
        {
            'Chronos.EventLogger': mock.MagicMock(),
            'Chronos.Core': mock.MagicMock()
        })
        self.patcher.start()

        global CircularReferenceException
        from Chronos.Dependency import AggregateSynchronizationManager, CircularReferenceException
        self.synchronizationManager = AggregateSynchronizationManager()

    def tearDown(self):
        self.patcher.stop()

    def test_AddSynchronizationProxies_with_duplicate_name_should_raise_ValueError(self):
        self.synchronizationManager.AddSynchronizationProxy('duplicate', mock.MagicMock(), [])
        self.assertRaises(ValueError, self.synchronizationManager.AddSynchronizationProxy, 'duplicate', mock.MagicMock(), [])

    def test_AddSynchronizationProxies_with_circular_dependency_should_raise_CircularReferenceException(self):
        self.synchronizationManager.AddSynchronizationProxy('circular1', mock.MagicMock(), ['circular2'])
        self.assertRaises(CircularReferenceException, self.synchronizationManager.AddSynchronizationProxy, 'circular2', mock.MagicMock, ['circular1'])

    @mock.patch('Chronos.Dependency.EventLogger')
    def test_AddSynchronizationProxies_with_dead_dependency_should_not_raise(self, mockEventLogger):
        self.synchronizationManager.AddSynchronizationProxy('dependent', None, ['deadDependency'])
        self.synchronizationManager.AddSynchronizationProxy('deadDependency', mock.MagicMock(), [])
        self.assertEqual(mockEventLogger.LogExceptionAuto.call_count, 1)

    def test_AddSynchronizationProxies_with_dependencies_added_first_should_assign_proxies(self):
        firstDependencyMock = mock.MagicMock()
        secondDependencyMock = mock.MagicMock()
        dependentMock = mock.MagicMock()
        self.synchronizationManager.AddSynchronizationProxy('FirstDependency', firstDependencyMock, [])
        self.synchronizationManager.AddSynchronizationProxy('SecondDependency', secondDependencyMock, [])
        self.synchronizationManager.AddSynchronizationProxy('Dependent', dependentMock, ['FirstDependency', 'SecondDependency'])

        self.assertEqual(dependentMock.AssignSynchronizationProxy.call_args_list, [mock.call('FirstDependency', firstDependencyMock),
                                                                                   mock.call('SecondDependency', secondDependencyMock)])
        self.assertEqual(firstDependencyMock.AssignSynchronizationProxy.call_count, 0)
        self.assertEqual(secondDependencyMock.AssignSynchronizationProxy.call_count, 0)

    def test_AddSynchronizationProxies_with_dependencies_added_out_of_order_should_assign_proxies(self):
        independentMock = mock.MagicMock()
        interimDependentMock = mock.MagicMock()
        dependentMock = mock.MagicMock()
        self.synchronizationManager.AddSynchronizationProxy('Interim', interimDependentMock, ['Independent'])
        self.synchronizationManager.AddSynchronizationProxy('Dependent', dependentMock, ['Interim'])
        self.synchronizationManager.AddSynchronizationProxy('Independent', independentMock, [])

        dependentMock.AssignSynchronizationProxy.assert_called_once_with('Interim', interimDependentMock)
        interimDependentMock.AssignSynchronizationProxy.assert_called_once_with('Independent', independentMock)
        self.assertEqual(independentMock.AssignSynchronizationProxy.call_count, 0)

    def test_DeleteSynchronizationProxy_should_remove_proxy_from_all_dependencies(self):
        independentMock = mock.MagicMock()
        interimDependentMock = mock.MagicMock()
        dependentMock = mock.MagicMock()
        self.synchronizationManager.AddSynchronizationProxy('Interim', interimDependentMock, ['Independent'])
        self.synchronizationManager.AddSynchronizationProxy('Dependent', dependentMock, ['Independent'])
        self.synchronizationManager.AddSynchronizationProxy('Independent', independentMock, [])
        self.synchronizationManager.DeleteSynchronizationProxy('Independent')

        dependentMock.DeleteSynchronizationProxy.assert_called_once_with('Independent')
        interimDependentMock.DeleteSynchronizationProxy.assert_called_once_with('Independent')

    @mock.patch('Chronos.Dependency.EventLogger')
    def test_multiple_DeleteSynchronizationProxy_calls_should_not_attempt_multiple_deletions(self, mockEventLogger):
        independentMock = mock.MagicMock()
        interimDependentMock = mock.MagicMock()
        dependentMock = mock.MagicMock()
        self.synchronizationManager.AddSynchronizationProxy('Independent', independentMock, [])
        self.synchronizationManager.AddSynchronizationProxy('Interim', interimDependentMock, ['Independent'])
        self.synchronizationManager.AddSynchronizationProxy('Dependent', dependentMock, ['Independent'])
        self.synchronizationManager.DeleteSynchronizationProxy('Independent')
        self.synchronizationManager.DeleteSynchronizationProxy('Independent')

        dependentMock.DeleteSynchronizationProxy.assert_called_once_with('Independent')
        interimDependentMock.DeleteSynchronizationProxy.assert_called_once_with('Independent')
        self.assertEqual(mockEventLogger.LogExceptionAuto.call_count, 0)
        self.assertEqual(mockEventLogger.LogWarningAuto.call_count, 0)
