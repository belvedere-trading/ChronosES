"""@file
Logic for handling synchronization dependencies between Chronos Aggregates.

The AggregateSynchronizationManager is the entrypoint into dependency resolution.
The logic defined in this module meant for use almost exclusive by the Chronos::Gateway module.
@addtogroup Chronos
@{
"""
from collections import defaultdict, namedtuple
from threading import Lock

from Chronos.Core import ChronosSemanticException, Event
from Chronos.EventLogger import EventLogger

AggregateDependency = namedtuple('AggregateDependency', ['aggregateName', 'destinationProperty', 'eventSourceProperty', 'aggregateSourceProperty'])

class dependent_upon(object): #pylint: disable=C0103
    """An optional class decorator for use with Chronos::Core::Event subclasses specifying an external dependency on another Aggregate instance.

    Use of this decorator implies that instances of the decorated Event can only be processed after acquiring a lock on an external dependency.
    If the external dependency is unavailable (or cannot be resolved), then the Event will fail processing without attempting to execute
    its internal logic.
    """
    def __init__(self, aggregateName, destinationProperty, eventSourceProperty=None, aggregateSourceProperty=None):
        if eventSourceProperty is None and aggregateSourceProperty is None:
            raise ChronosSemanticException('Either eventSourceProperty or aggregateSourceProperty must be set')
        if eventSourceProperty is not None and aggregateSourceProperty is not None:
            raise ChronosSemanticException('Only one of eventSourceProperty or aggregateSourceProperty can be set simultaneously')
        self.aggregateName = aggregateName
        self.destinationProperty = destinationProperty
        self.eventSourceProperty = eventSourceProperty
        self.aggregateSourceProperty = aggregateSourceProperty

    def __call__(self, cls):
        if not issubclass(cls, Event):
            raise ChronosSemanticException('dependent_upon can only be applied to Chronos.Core.Event subclasses')
        if not hasattr(cls.Proto, self.destinationProperty):
            raise ChronosSemanticException('Invalid destinationProperty {0}'.format(self.destinationProperty))
        if self.eventSourceProperty is not None and not hasattr(cls.Proto, self.eventSourceProperty):
            raise ChronosSemanticException('Invalid eventSourceProperty {0}'.format(self.eventSourceProperty))
        if self.aggregateSourceProperty is not None and not hasattr(cls.Aggregate.Proto, self.aggregateSourceProperty):
            raise ChronosSemanticException('Invalid aggregateSourceProperty {0}'.format(self.aggregateSourceProperty))
        if len([dependency for dependency in cls.Dependencies if dependency.aggregateName == self.aggregateName]) > 0:
            raise ChronosSemanticException('Cannot satisfy duplicate dependency on Aggregate {0}'.format(self.aggregateName))

        cls.Aggregate.Dependencies.add(self.aggregateName)
        dependency = AggregateDependency(self.aggregateName, self.destinationProperty, self.eventSourceProperty, self.aggregateSourceProperty)
        cls.Dependencies.append(dependency)

        return cls

class CircularReferenceException(Exception):
    pass

class AggregateSynchronizationManager(object):
    """Orchestrates a single-depth dependency tree of Chronos::Gateway::ChronosProcess instances.

    The primary purpose of this class is to ensure that instances can be registered in any order; as long
    as all dependencies are registered before an event must be processed, the dependencies will be properly
    resolved.
    """
    def __init__(self):
        self.aggregateProxies = {}
        self.reverseDependencyMapping = defaultdict(set)
        self.lock = Lock()

    def AddSynchronizationProxy(self, name, proxy, dependencies):
        """Adds a proxy for the provided Aggregate to the synchronization manager for later assignment.

        @param name The name of the Aggregate corresponding to @p proxy.
        @param proxy The Chronos::Gateway::ChronosProcess instance that should marshal calls for the Aggregate specified by @p name.

        @throws ValueError If a proxy already exists for @p name.
        """
        with self.lock:
            if name in self.aggregateProxies:
                EventLogger.LogErrorAuto(self, 'Inconsistent state detected', 'Attempted to add duplicate proxy',
                                         tags={'Aggregate': name})
                raise ValueError('Cannot add duplicate proxy for {0}'.format(name))
            for dependency in dependencies:
                if dependency in self.reverseDependencyMapping[name]:
                    raise CircularReferenceException('Dependency {0} relies on {1}'.format(dependency, name))
            self.aggregateProxies[name] = proxy
            self._assignSynchronizationProxies(name, proxy, dependencies)
        for dependency in self.reverseDependencyMapping[name]:
            try:
                self.aggregateProxies[dependency].AssignSynchronizationProxy(name, proxy)
            except KeyError: # Other proxy isn't registered yet, it'll get assigned when it registers
                pass
            except Exception: # Probably a dead proxy somehow - just log
                EventLogger.LogExceptionAuto(self, 'Unable to assign individual synchronization proxy',
                                             tags={'Aggregate': name, 'Dependency': dependency})

    def _assignSynchronizationProxies(self, name, chronosProcess, dependencies):
        """Associates synchronization proxies for later use in event processing.

        If a dependency cannot be found, the error will be logged, but no Exception will be raised.
        Chronos::Gateway::ChronosProcess is expected to gracefully handle a missing dependency proxy by persisting
        an event failure.

        @param name The name of the Aggregate corresponsing to @p chronosProcess.
        @param chronosProcess The Chronos::Gateway::ChronosProcess instance that should have proxies assigned to it.
        @param dependencies A list of strings specifying the Aggregates that @p chronosProcess depends on.
        """
        for dependency in dependencies:
            self.reverseDependencyMapping[dependency].add(name)
            try:
                proxy = self.aggregateProxies[dependency]
                chronosProcess.AssignSynchronizationProxy(dependency, proxy)
            except KeyError:
                EventLogger.LogWarningAuto(self, 'Unable to fully resolve synchronization proxies',
                                           'Dependency was not found', tags={'Aggregate': name, 'Dependency': dependency})
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Unable to fully resolve synchronization proxies',
                                             'Assignment of individual synchronization proxy raised an unexpected exception',
                                             tags={'Aggregate': name, 'Dependency': dependency})

    def DeleteSynchronizationProxy(self, name):
        """Deletes the proxy for the provided Aggregate from the synchronization manager and all dependents.

        @param name The name of the Aggregate that should be deleted.
        """
        with self.lock:
            try:
                del self.aggregateProxies[name]
                for dependency in self.reverseDependencyMapping[name]:
                    try:
                        dependentProxy = self.aggregateProxies[dependency]
                        dependentProxy.DeleteSynchronizationProxy(name)
                    except KeyError:
                        EventLogger.LogWarningAuto(self, 'Inconsistent state detected', 'Attempted to delete proxy from nonexistant dependency',
                                                   tags={'Aggregate': name, 'Dependency': dependency})
                    except Exception:
                        EventLogger.LogExceptionAuto(self, 'Failed to delete individual dependent synchronization proxy',
                                                     tags={'Aggregate': name, 'Dependency': dependency})
                for maybeDependentSet in self.reverseDependencyMapping.itervalues():
                    maybeDependentSet.discard(name)
            except KeyError:
                pass
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Failed to delete synchronization proxy',
                                             tags={'Aggregate': name})

class DependenciesNotReleased(Exception):
    pass

class MissingDependency(Exception):
    pass

class ChronosProcessSynchronizer(object):
    """A utility class that encapsulates remote synchronization logic.

    This class is meant for use by Chronos::Gateway::ChronosProcess.
    """
    def __init__(self):
        self.synchronizationProxies = {}
        self.remoteLocks = []

    def AssignSynchronizationProxy(self, aggregateName, proxy):
        """Adds a proxy to the internal registry for future use.

        @param aggregateName The name of the Aggregate related to @p proxy.
        @param proxy The Chronos::Gateway::ChronosProcess instance running in another physical process.
        """
        self.synchronizationProxies[aggregateName] = proxy

    def DeleteSynchronizationProxy(self, aggregateName):
        """Removes a proxy from the internal registry.

        @param aggregateName The name of the Aggregate that should be disregarded.
        """
        self.synchronizationProxies.pop(aggregateName, None)

    def AcquireDependencies(self, aggregate, event):
        """Acquires remote locks on all dependencies required by @p event.

        This method requires that @p aggregate is locked prior to execution.

        @param aggregate The Aggregate instance that is going to have @p event applied to it.
        @param event The Event instance that is going to be applied to @p aggregate.

        @throws PossibleDeadlock If @p aggregate is not locked prior to execution.
        @throws DependenciesNotReleased If AcquireDependencies is called twice without a ReleaseDependencies call in between.
        @throws MissingDependency If a required dependency of @p event is not available.
        """
        if self.remoteLocks:
            raise DependenciesNotReleased('Dependencies must be released before acquiring')
        try:
            for dependency in event.Dependencies:
                EventLogger.LogInformationAuto(self, 'Acquiring remote dependency',
                                               tags={'Aggregate': aggregate.__class__.__name__,
                                                     'Dependency': dependency.aggregateName})
                try:
                    proxy = self.synchronizationProxies[dependency.aggregateName]
                except KeyError:
                    raise MissingDependency(dependency.aggregateName)
                if dependency.eventSourceProperty is not None:
                    aggregateId = getattr(event, dependency.eventSourceProperty)
                else:
                    aggregateId = getattr(aggregate, dependency.aggregateSourceProperty)
                version = proxy.Acquire(aggregateId)
                self.remoteLocks.append((proxy, aggregateId))
                setattr(event, dependency.destinationProperty, version)
        except Exception:
            self.ReleaseDependencies()
            raise

    def ReleaseDependencies(self):
        """Releases all remote locks currently held by this instance.
        """
        for proxy, aggregateId in self.remoteLocks:
            try:
                proxy.Release(aggregateId)
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Failed to release remote lock')
        del self.remoteLocks[:]
