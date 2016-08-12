"""@file
Interaction logic for the Chronos API.

The ChronosGateway class is the external entrypoint into the Chronos service.
When requests are received from clients, ChronosGateway routes them to their
appropriate ChronosProcess instances.
@addtogroup Chronos
@{
"""
import copy
import os
import pickle
import sys
import time
import Queue
from collections import namedtuple
from datetime import datetime
from multiprocessing.managers import BaseManager
from threading import Lock, Thread, Event

from Chronos.EventLogger import EventLogger
from Chronos.Infrastructure import InfrastructureProvider, ConfigurablePlugin, TransportType

from Chronos.Chronos_pb2 import ChronosRegistrationResponse, ChronosManagementNotification, ChronosQueryResponse, ChronosTagList

from Chronos.Core import AggregateLogicCompiler, ValidationError
from Chronos.Dependency import ChronosProcessSynchronizer

class ChronosGatewayException(Exception):
    """An Exception indicating that Chronos has encountered an error from
    which it is unable to recover. This Exception could be thrown due to
    network events, logic errors, or inconsistent state."""
    pass

ChronosEventQueueItem = namedtuple('ChronosEventQueueItem', ['aggregateId', 'eventType', 'eventProto', 'requestId', 'senderId',
                                                             'receivedTimestamp', 'tag', 'tagExpiration'])
ChronosEventQueueTransaction = namedtuple('ChronosEventQueueTransaction', ['events'])

class ChronosQueryItem(object):
    """A simple wrapper that unifies the three possible Chronos query types
    into a single request object. A ChronosProcess instance is able to use the
    ChronosQueryItem to return a subset of snapshots that a calling client is
    interested in."""
    def __init__(self, aggregateId=None, indexKeys=None):
        if aggregateId is not None and indexKeys is not None:
            raise ChronosGatewayException('Only one of aggregateId or indexKeys can be set')

        self.aggregateId = aggregateId
        self.indexKeys = indexKeys


class ChronosProcess(object):
    """The main logical component of the ChronosGateway. Each aggregate that is
    registered with the service will have its own instance of ChronosProcess; each
    instance of ChronosProcess runs within its own physical process on the server.

    ChronosProcess has three main pieces of functionality: asynchronous event queue
    processing, synchronous query handling, and tag management. Query hanlding is
    separated from the other operations and cannot affect writing. Tags can optionally
    be specified along with specific events (in which case a failing tag creation will
    fail the entire event).

    This class is threadsafe and all exceptions should be logged and handled internally."""
    def __init__(self, aggregateClass, aggregateRegistry, logicVersion, coreProviderGenerator):
        self.aggregateClass = aggregateClass
        self.aggregateRegistry = aggregateRegistry
        self.logicVersion = logicVersion
        self.coreProviderGenerator = coreProviderGenerator
        self.processSynchronizer = ChronosProcessSynchronizer()
        self.lock = Lock()
        self.queue = Queue.Queue()
        self.queryEvent = Event()
        self.queryEvent.set()
        self.isRunning = False
        self.processingThread = None
        self.coreProvider = None
        self.indexStore = None
        self.repository = None
        self.index = None
        self.processor = None
        self.maxRequestId = 0

    def Startup(self):
        """Creates the Chronos Core instances required for processing and starts
        the ChronosProcess::EventLoop in a background thread."""
        EventLogger.LogInformationAuto(self, 'Starting Chronos process',
                                       tags={'Aggregate': self.aggregateClass.__name__})
        with self.lock:
            try:
                if self.isRunning:
                    return

                self.coreProvider = self.coreProviderGenerator(self.aggregateClass)
                self.indexStore = self.coreProvider.GetIndexStore()
                self.repository = self.coreProvider.GetRepository()
                self.processor = self.coreProvider.GetProcessor()

                self.isRunning = True
                self.processingThread = Thread(target=self.EventLoop, name=self.aggregateClass.__name__)
                self.processingThread.start()
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Unable to start Chronos process',
                                             tags={'Aggregate': self.aggregateClass.__name__})
                excInfo = sys.exc_info()
                raise ChronosGatewayException, excInfo[1], excInfo[2]


    def Shutdown(self):
        """Cleans up the Chronos Core instances owned by this instance and stops
        the ChronosProcess::EventLoop. The event loop will complete processing of all
        items in the queue before it allows this method to return."""
        EventLogger.LogInformationAuto(self, 'Shutting down Chronos process',
                                       tags={'Aggregate': self.aggregateClass.__name__})
        with self.lock:
            try:
                if not self.isRunning:
                    return

                self.isRunning = False
                self.queue.put(None)
                self.processingThread.join()
                self.processingThread = None

                self.coreProvider.Dispose()
                self.coreProvider = None
                self.repository = None
                self.processor = None
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Error shutting down Chronos process',
                                             tags={'Aggregate': self.aggregateClass.__name__})
                excInfo = sys.exc_info()
                raise ChronosGatewayException, excInfo[1], excInfo[2]

    def AssignSynchronizationProxy(self, aggregateName, proxy):
        self.processSynchronizer.AssignSynchronizationProxy(aggregateName, proxy)

    def DeleteSynchronizationProxy(self, aggregateName):
        self.processSynchronizer.DeleteSynchronizationProxy(aggregateName)

    def SendEvent(self, eventQueueItem):
        """Enqueues in an event for processing. Raises ChronosGatewayException if
        the instance of ChronosProcess is not running."""
        if not self.isRunning:
            raise ChronosGatewayException('Cannot enqueue event - process not running')
        self.queue.put(eventQueueItem)

    def _tryCreateSqliteSession(self):
        try:
            self.index = self.indexStore.GetSession()
            return True
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Critical failure encountered while initializing', 'Failed to acquire SQLite session',
                                         tags={'Aggregate': self.aggregateClass.__name__})
            try:
                self.Shutdown()
            except ChronosGatewayException:
                pass # Already logged, not much we can do here
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Impossible exception raised',
                                             'Shutdown raised an exception of type other than ChronosGatewayException',
                                             tags={'Aggregate': self.aggregateClass.__name__})
            return False

    def _verifyEventPersistenceCheckpoint(self):
        try:
            eventPersistenceCheckpoint = self.repository.GetEventPersistenceCheckpoint()
            if eventPersistenceCheckpoint is None:
                return
            eventPersistenceCheckpoint.VerifyCheckpoint(self.indexStore, self.index, self.repository)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Checkpoint verification failed',
                                         tags={'Aggregate': self.aggregateClass.__name__})

    def EventLoop(self):
        """Event queue processing. This method should always be run in a background
        thread; it will not return until the ChronosProcess instance is shut down."""
        if not self.isRunning:
            raise ChronosGatewayException('Cannot start event loop - process not running')
        EventLogger.LogInformationAuto(self, 'Starting Chronos event loop',
                                       tags={'Aggregate': self.aggregateClass.__name__})

        wasSessionCreated = self._tryCreateSqliteSession()
        if not wasSessionCreated:
            EventLogger.LogErrorAuto(self, 'Failed to create index session')
            return
        self._verifyEventPersistenceCheckpoint()

        while self.isRunning or not self.queue.empty():
            try:
                eventQueueItem = self.queue.get(True)
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Unexpected exception while polling queue')
                time.sleep(1) # To prevent log spam if the queue is corrupted
                continue
            if eventQueueItem is None:
                EventLogger.LogInformationAuto(self, 'Aborting event loop', 'Poison pill pulled from event queue')
                break

            self._doProcessEvent(eventQueueItem)
        try:
            self.index.Rollback()
            self.index.Close()
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Unable to finalize SQLite session',
                                         tags={'Aggregate': self.aggregateClass.__name__})
        EventLogger.LogInformationAuto(self, 'Chronos event loop exiting',
                                       tags={'Aggregate': self.aggregateClass.__name__})

    def Begin(self):
        self.repository.Begin()
        self.processor.Begin()
        self.index.Begin()

    def Commit(self):
        self.index.Commit()
        self.processor.Commit()
        self.repository.Commit()

    def Rollback(self):
        self.index.Rollback()
        self.processor.Rollback()
        self.repository.Rollback()

    def _doProcessEvent(self, eventQueueItem):
        try:
            self.Begin()
            if isinstance(eventQueueItem, ChronosEventQueueItem):
                self.ProcessEvent(eventQueueItem)
            elif isinstance(eventQueueItem, ChronosEventQueueTransaction):
                for event in eventQueueItem.events:
                    if not self.ProcessEvent(event):
                        break
            else:
                EventLogger.LogErrorAuto(self, 'Skipping single event queue item', 'Item has unknown type',
                                         tags={'Type': type(eventQueueItem)})
                return
            self.Commit()
            # If there's nothing to currently process, we don't have a reason to keep
            # items in the persistence buffer
            try:
                self.queryEvent.clear()
                if self.processor.FlushPersistenceBuffer(self.aggregateClass, shouldForce=self.queue.empty()):
                    try:
                        self.index.Begin()
                        self.indexStore.UpdateCheckpoint(self.maxRequestId, self.index)
                        self.index.Commit()
                    except Exception:
                        EventLogger.LogExceptionAuto(self, 'Failed to update index persistence checkpoint',
                                                     tags={'Aggregate': self.aggregateClass.__name__})
                        self.index.Rollback()
                    self.index.Commit()
            finally:
                self.queryEvent.set()
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Critical error - possibly failing event notification',
                                         'Catastrophic failure encountered during single event processing',
                                         tags={'Aggregate': self.aggregateClass.__name__, 'RequestId': eventQueueItem.requestId,
                                               'EventType': eventQueueItem.eventType, 'AggregateId': eventQueueItem.aggregateId})
            self.Rollback()

    def ProcessEvent(self, eventQueueItem):
        """Determines the corresponding Python class to an eventType sent from
        a client and applies that type's logic to the specified aggregate. This
        method makes use of the AggregateRegistry created by the Chronos Core
        metaclasses at import time."""
        try:
            existingAggregate = self.GetUnexpiredAggregate(eventQueueItem)
            # Deep copy and emplace are used so that a failed validation doesn't leave the aggregate in a bad state
            aggregateCopy = copy.deepcopy(existingAggregate)
            with existingAggregate.lock:
                aggregate, bufferItem = self.ApplyEvent(aggregateCopy, eventQueueItem)

                if existingAggregate.version == 1 or aggregate.HasDivergedFrom(existingAggregate):
                    self.indexStore.ReindexAggregate(aggregate, self.index)
                    self.processor.ProcessIndexDivergence(self.aggregateClass, aggregate.aggregateId)

                if eventQueueItem.tag is not None:
                    self.indexStore.IndexTag(eventQueueItem.tag, aggregateCopy.aggregateId, self.index)
                    self.processor.ProcessTag(aggregate, eventQueueItem.tag, eventQueueItem.tagExpiration, long(time.time()))

                self.processor.EnqueueForPersistence(bufferItem)
                self.repository.Emplace(aggregate)
                self.maxRequestId = eventQueueItem.requestId
                return True
        except Exception as ex:
            if not isinstance(ex, ValidationError):
                EventLogger.LogExceptionAuto(self, 'Error processing single event',
                                             tags={'AggregateId': eventQueueItem.aggregateId, 'EventType': eventQueueItem.eventType,
                                                   'Aggregate': self.aggregateClass.__name__, 'Tag': eventQueueItem.tag})
            try:
                self.Rollback()
                self.processor.ProcessFailure(eventQueueItem, self.aggregateClass, ex)
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Critical error - failing event notification',
                                             'EventProcessor unable to send failure notification',
                                             tags={'AggregateId': eventQueueItem.aggregateId, 'EventType': eventQueueItem.eventType,
                                                   'Aggregate': self.aggregateClass.__name__, 'Tag': eventQueueItem.tag})
            return False
        finally:
            self.processSynchronizer.ReleaseDependencies()

    def GetUnexpiredAggregate(self, eventQueueItem):
        """Attempts to retrieve the aggregate instance specified by eventQueueItem.

        @throw ChronosCoreException if eventQueueItem specifies a non-existant aggregate instance
        @throw ChronosGatewayException if eventQueueItem specifies an expired aggregate instance"""

        if eventQueueItem.aggregateId == 0:
            aggregate = self.repository.Create()
        else:
            aggregate = self.repository.Get(eventQueueItem.aggregateId)

        if aggregate.expiration > 0 and eventQueueItem.receivedTimestamp > aggregate.expiration:
            raise ChronosGatewayException('Unable to process event - aggregate instance expired')

        return aggregate

    def ApplyEvent(self, aggregate, eventQueueItem):
        eventGenerator = self.aggregateRegistry[eventQueueItem.eventType]
        event = eventGenerator(aggregate.version, self.logicVersion)
        event.proto.ParseFromString(eventQueueItem.eventProto)
        self.processSynchronizer.AcquireDependencies(aggregate, event)

        return self.processor.Process(eventQueueItem, aggregate, event)

    def Acquire(self, aggregateId):
        aggregate = self.repository.AtomicGet(aggregateId)
        # If we can't acquire the lock on the first attempt, it's possible that an Event is currently being processed
        # Because that could lead to an Emplace, we need to ensure that we're locking on the most recent instance.
        # TODO(jkaye): Once Emplace is removed in favor of Aggregate commit/rollback functionality, this can be simplified
        if not aggregate.lock.acquire(False):
            aggregate.lock.acquire()
            aggregate.lock.release()
            return self.Acquire(aggregateId)

        return aggregate.version - 1

    def Release(self, aggregateId):
        self.repository.Get(aggregateId).lock.release()

    def Query(self, queryItem):
        """Processes a ChronosQueryItem to retrieve aggregate snapshots requested
        by a client."""
        if not self.isRunning:
            raise ChronosGatewayException('Cannot perform query - process not running')

        self.queryEvent.wait()
        with self.lock:
            try:
                if queryItem.aggregateId is not None:
                    return [self.repository.Get(queryItem.aggregateId)]
                elif queryItem.indexKeys is not None:
                    return self.repository.GetFromIndex(**queryItem.indexKeys)
                else:
                    return self.repository.GetAll()
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Error processing single query',
                                             tags={'AggregateId': queryItem.aggregateId, 'IndexKeys': queryItem.indexKeys,
                                                   'Aggregate': self.aggregateClass.__name__})
                excInfo = sys.exc_info()
                raise ChronosGatewayException, excInfo[1], excInfo[2]

    def GetAllTags(self):
        try:
            tagList = ChronosTagList()
            for tag in self.indexStore.GetAllTags():
                tagList.tags.append(tag.tag) #pylint: disable=E1101
            return tagList
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error retrieving tags',
                                         tags={'Aggregate': self.aggregateClass.__name__})
            excInfo = sys.exc_info()
            raise ChronosGatewayException, excInfo[1], excInfo[2]

    def GetByTag(self, tag, aggregateId):
        try:
            return self.repository.GetTag(aggregateId, tag)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error retrieving tag',
                                         tags={'Aggregate': self.aggregateClass.__name__,
                                               'Tag': tag,
                                               'AggregateId': aggregateId})
            excInfo = sys.exc_info()
            raise ChronosGatewayException, excInfo[1], excInfo[2]


class ChronosProcessManager(BaseManager):
    """The multiprocessing.Manager that marshalls instances of ChronosProcess into
    their own physical processes on the server."""
    def Shutdown(self):
        self.shutdown()

ChronosProcessManager.register('ChronosProcess', ChronosProcess)

class ChronosRegistryItem(object):
    """Associates a ChronosProcessManager with its ChronosProcess instance.
    These items are retained by the ChronosGateway so that the gateway can be
    cleaned up in its entirety if necessary."""
    def __init__(self):
        self.lock = Lock()
        self.isRunning = False
        self.manager = None
        self.process = None
        self.module = None

    def Startup(self, manager, process, module):
        """Starts the underlying ChronosProcess."""
        if self.isRunning:
            raise ChronosGatewayException('Cannot replace running process')
        self.manager = manager
        self.process = process
        self.module = module
        self.process.Startup()
        self.isRunning = True

    def Shutdown(self):
        """Shuts down both the underlying ChronosProcess and its manager."""
        if self.process is not None:
            self.process.Shutdown()
            self.process = None
        if self.manager is not None:
            self.manager.Shutdown()
            self.manager = None
        self.module = None
        self.isRunning = False


class StatisticsLogger(object):
    """Aggregates and logs statistics about Chronos operations."""
    LoggingInterval = 30 # Seconds
    def __init__(self):
        self.isRunning = True
        self.aggregatedEventStats = {}
        self.eventLock = Lock()
        self.aggregatedQueryStats = {}
        self.queryLock = Lock()
        self.lastLogTime = datetime.utcnow()
        self.loggingThread = Thread(target=self.LogAggregatedStatistics)
        self.loggingThread.daemon = True
        self.loggingThread.start()

    def LogAggregatedStatistics(self):
        while self.isRunning:
            try:
                time.sleep(1)
                now = datetime.utcnow()
                if (now - self.lastLogTime).seconds >= self.LoggingInterval:
                    self.lastLogTime = now
                    self._log()
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Exception encountered in statistics logging thread')

    def _log(self):
        with self.eventLock:
            copiedEventStats = self.aggregatedEventStats
            self.aggregatedEventStats = {}
        with self.queryLock:
            copiedQueryStats = self.aggregatedQueryStats
            self.aggregatedQueryStats = {}
        for aggregateName, events in copiedEventStats.iteritems():
            events['Aggregate'] = aggregateName
            EventLogger.LogInformationAuto(self, 'Chronos event statistics', tags=events)
        for aggregateName, numQueries in copiedQueryStats.iteritems():
            EventLogger.LogInformationAuto(self, 'Chronos query statistics',
                                           tags={'NumQueries': numQueries, 'Aggregate': aggregateName})

    def LogEvent(self, aggregateName, eventType):
        eventType = '{0}Count'.format(eventType)
        with self.eventLock:
            if aggregateName not in self.aggregatedEventStats:
                self.aggregatedEventStats[aggregateName] = {eventType: 1}
            elif eventType not in self.aggregatedEventStats[aggregateName]:
                self.aggregatedEventStats[aggregateName][eventType] = 1
            else:
                self.aggregatedEventStats[aggregateName][eventType] += 1

    def LogQuery(self, aggregateName):
        with self.queryLock:
            if aggregateName not in self.aggregatedQueryStats:
                self.aggregatedQueryStats[aggregateName] = 1
            else:
                self.aggregatedQueryStats[aggregateName] += 1

    def Shutdown(self):
        EventLogger.LogInformationAuto(self, 'Shutting down')
        self.isRunning = False
        self.loggingThread.join()
        self._log()

class ChronosGateway(object):
    """The main server implementation. The ChronosGateway handles registration of aggregate/event
    logic from clients and routes event/query requests to the proper ChronosProcess."""
    RegisteredModulesPath = 'modules.dat'
    def __init__(self, eventStore, synchronizationManager, coreProviderGenerator):
        self.eventStore = eventStore
        self.synchronizationManager = synchronizationManager
        self.coreProviderGenerator = coreProviderGenerator
        self.logicCompiler = AggregateLogicCompiler(self.eventStore)
        self.statisticsLogger = StatisticsLogger()
        self.requestIdLock = Lock()
        self.registryLock = Lock()
        self.registry = {}
        self.requestIdCounter = 1
        sys.path.append('.')
        self.LoadRegistry()

    def LoadRegistry(self):
        """If a previous instance of the ChronosGateway persisted registry information to disk,
        this method will re-register all previously registered aggregate logic.

        This method is meant to be called during Gateway startup only."""
        EventLogger.LogInformationAuto(self, 'Loading previously registered modules')
        with self.registryLock:
            try:
                if not os.path.exists(self.RegisteredModulesPath):
                    EventLogger.LogInformationAuto(self, 'Registry not loaded',
                                                   'No previously registered module information found')
                    return

                with open(self.RegisteredModulesPath, 'r') as moduleFile:
                    moduleNames = pickle.load(moduleFile)
                for moduleName in moduleNames:
                    try:
                        module = AggregateLogicCompiler.ImportAggregateLogic(moduleName)
                        logicVersion, _ = self.eventStore.GetLatestAggregateLogic(module.AggregateClass).items()[0]
                        self.registry[moduleName] = ChronosRegistryItem()
                        self.AddRegistryItem(logicVersion, module)
                    except Exception:
                        EventLogger.LogExceptionAuto(self, 'Error loading single module',
                                                     tags={'ModuleName': moduleName})
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Error loading registry')

    def SaveRegistry(self):
        """Persists registry information to disk for reloading upon gateway startup."""
        EventLogger.LogInformationAuto(self, 'Saving registered modules')
        with self.registryLock:
            try:
                with open(self.RegisteredModulesPath, 'w') as moduleFile:
                    pickle.dump([key for (key, value) in self.registry.iteritems() if value.isRunning], moduleFile)
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Error saving registered modules')

    @staticmethod
    def CreateRegistrationResponse(errorMessage='', indexedAttributes=None):
        response = ChronosRegistrationResponse()
        if errorMessage:
            response.responseCode = ChronosRegistrationResponse.FAILURE #pylint: disable=E1101
            response.responseMessage = errorMessage
        else:
            if indexedAttributes is None:
                raise ChronosGatewayException('Cannot create successful registration response without indexedAttributes')
            response.responseCode = ChronosRegistrationResponse.SUCCESS #pylint: disable=E1101
            response.indexedAttributes.extend(sorted(indexedAttributes)) #pylint: disable=E1101

        return response.SerializeToString()

    def Register(self, request):
        """Compiles protobuf messages and Python logic for Aggregate registration, then
        updates the current state of the server. If the supplied Aggregate was previously
        registered with the server, its existing event queue will be exhausted before the
        new logic is registered (though new event requests will not be honored during this
        time)."""
        EventLogger.LogInformationAuto(self, 'Registering Aggregate',
                                       tags={'Aggregate': request.aggregateName,
                                             'PythonFileContents': request.aggregateLogic.pythonFileContents,
                                             'ProtoFileContents': request.aggregateLogic.protoFileContents})
        with self.registryLock:
            registryItem = self.registry.get(request.aggregateName, None)
            if registryItem is None:
                registryItem = ChronosRegistryItem()
                self.registry[request.aggregateName] = registryItem

        with registryItem.lock:
            self.synchronizationManager.DeleteSynchronizationProxy(request.aggregateName)
            registryItem.Shutdown()
            try:
                logicVersion, module = self.logicCompiler.Compile(request.aggregateName, request.aggregateLogic)
                self.AddRegistryItem(logicVersion, module)
                self.SaveRegistry()
                managementNotification = ChronosManagementNotification(notificationType=ChronosManagementNotification.REGISTERED, #pylint: disable=E1101
                                                                       indexedAttributes=sorted(module.AggregateClass.IndexedAttributes))
                self.eventStore.PublishManagementNotification(module.AggregateClass, managementNotification)

                return self.CreateRegistrationResponse(indexedAttributes=module.AggregateClass.IndexedAttributes)
            except Exception as exc:
                errorMessage = str(exc)
                EventLogger.LogExceptionAuto(self, 'Aggregate registration failed', errorMessage,
                                             tags={'AggregateName': request.aggregateName})
                return self.CreateRegistrationResponse(errorMessage=errorMessage)

    def AddRegistryItem(self, logicVersion, module):
        """Creates a new ChronosProcessManager and ChronosProcess for the
        module created from a ChronosRegistrationRequest."""
        manager = ChronosProcessManager()
        manager.start()
        process = manager.ChronosProcess(module.AggregateClass, #pylint: disable=E1101
                                         module.AggregateRegistry, logicVersion, self.coreProviderGenerator)
        self.synchronizationManager.AddSynchronizationProxy(module.AggregateClass.__name__, process, module.AggregateClass.Dependencies)
        self.registry[module.__name__].Startup(manager, process, module)

    def GetRegistryItem(self, aggregateName):
        """Retrieves the ChronosRegistryItem for the given aggregate.
        Raises ChronosGatewayException if the registry item could not be found."""
        with self.registryLock:
            registryItem = self.registry.get(aggregateName, None)
        if registryItem is None or not registryItem.isRunning:
            errorMessage = 'Aggregate not registered: {0}'.format(aggregateName)
            EventLogger.LogErrorAuto(self, errorMessage, tags={'Aggregate': aggregateName})
            raise ChronosGatewayException(errorMessage)

        return registryItem

    def Unregister(self, aggregateName):
        """Shuts down the ChronosProcessManager and ChronosProcess for the supplied aggregate."""
        EventLogger.LogInformationAuto(self, 'Unregistering aggregate',
                                       tags={'Aggregate': aggregateName})
        try:
            with self.registryLock:
                registryItem = self.registry.get(aggregateName, None)

            if registryItem is not None:
                with registryItem.lock:
                    try:
                        module = registryItem.module
                        self.synchronizationManager.DeleteSynchronizationProxy(aggregateName)
                        registryItem.Shutdown()
                        if module is not None:
                            managementNotification = ChronosManagementNotification(notificationType=ChronosManagementNotification.UNREGISTERED) #pylint: disable=E1101
                            self.eventStore.PublishManagementNotification(module.AggregateClass, managementNotification)
                    finally:
                        self.SaveRegistry()
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error unregistering aggregate',
                                         tags={'Aggregate': aggregateName})
            excInfo = sys.exc_info()
            raise ChronosGatewayException, excInfo[1], excInfo[2]

    def CheckStatus(self, aggregateName):
        """Returns a ChronosRegistrationResponse detailing the current status of @p aggregateName"""
        try:
            with self.registryLock:
                registryItem = self.registry.get(aggregateName, None)
            if registryItem is None:
                return self.CreateRegistrationResponse(errorMessage='Aggregate {0} not found in registry'.format(aggregateName))
            if not registryItem.isRunning:
                return self.CreateRegistrationResponse(errorMessage='Aggregate {0} not currently running'.format(aggregateName))
            return self.CreateRegistrationResponse(indexedAttributes=registryItem.module.AggregateClass.IndexedAttributes)
        except Exception as ex:
            EventLogger.LogExceptionAuto(self, 'Error checking aggregate status',
                                         tags={'Aggregate': aggregateName})
            return self.CreateRegistrationResponse(errorMessage=str(ex))

    def RouteEvent(self, request):
        """Creates a ChronosEventQueueItem and enqueues it for processing by the
        correct ChronosProcess instance."""
        return self._routeEvent(request)

    def RouteEventWithTag(self, request):
        """Creates a ChronosEventQueueItem and enqueues it for processing by the
        correct ChronosProcess instance. After the event is processed, the aggregate
        snapshot will be tagged with the provided tag."""
        return self._routeEvent(request.request, request.tag, request.tagExpiration)

    def RouteTransaction(self, transaction):
        """Creates a ChronosTransactionQueueItem and enqueues it for processing
        by the correct ChronosProcess instance.
        """
        if len(transaction.requests) == 0:
            EventLogger.LogWarningAuto(self, 'Rejecting transaction request', 'No events provided')
            raise ChronosGatewayException('Cannot process transaction - must provide at least one event')
        with self.requestIdLock:
            requestId = self.requestIdCounter
            self.requestIdCounter += 1
        transactionTime = long(time.time() * 1e9)
        events = []
        for tagRequest in transaction.requests:
            request = tagRequest.request
            aggregateName, eventType = request.eventType.split('.')
            event = ChronosEventQueueItem(request.aggregateId, eventType, request.eventProto, requestId,
                                          request.senderId, transactionTime,
                                          tagRequest.tag if tagRequest.tag else None,
                                          tagRequest.tagExpiration if tagRequest.tag else None)
            events.append(event)
        transaction = ChronosEventQueueTransaction(events)
        registryItem = self.GetRegistryItem(aggregateName)
        registryItem.process.SendEvent(transaction)

        return requestId

    def _routeEvent(self, request, tag=None, tagExpiration=None):

        aggregateName, eventType = request.eventType.split('.')
        registryItem = self.GetRegistryItem(aggregateName)

        with self.requestIdLock:
            requestId = self.requestIdCounter
            self.requestIdCounter += 1
            eventQueueItem = ChronosEventQueueItem(request.aggregateId, eventType,
                                                   request.eventProto, requestId, request.senderId,
                                                   long(time.time() * 1e9), tag, tagExpiration)
            registryItem.process.SendEvent(eventQueueItem)

        self.statisticsLogger.LogEvent(aggregateName, eventType)
        return requestId

    def GetAll(self, request):
        """Retrieves all snapshots for the supplied Aggregate."""
        queryItem = ChronosQueryItem()
        return self._performQuery(request.aggregateName, queryItem)

    def GetById(self, request):
        """Retrieves the snapshot for the supplied aggregate."""
        queryItem = ChronosQueryItem(aggregateId=request.aggregateId)
        return self._performQuery(request.aggregateName, queryItem)

    def GetByIndex(self, request):
        """Retrieves all snapshots for the supplied Aggregate that satisfy the
        supplied index key."""
        if len(request.indexKeys) % 2 != 0:
            raise ChronosGatewayException('Malformed index keys')

        indexKeys = dict(request.indexKeys[i:i + 2] for i in xrange(0, len(request.indexKeys), 2))
        queryItem = ChronosQueryItem(indexKeys=indexKeys)
        return self._performQuery(request.aggregateName, queryItem)

    def GetByTag(self, request):
        """Retrieves the tagged snapshot corresponding to the given request."""
        registryItem = self.GetRegistryItem(request.aggregateName)
        aggregate = registryItem.process.GetByTag(request.tag, request.aggregateId)
        response = ChronosQueryResponse()
        if aggregate is None:
            response.responseCode = ChronosQueryResponse.TAG_NOT_FOUND #pylint: disable=E1101
        else:
            self._addToQueryResponse(response, aggregate)

        return response.SerializeToString()

    def GetTags(self, aggregateName):
        """Retrieves all tags for the provided Aggregate."""
        registryItem = self.GetRegistryItem(aggregateName)
        return registryItem.process.GetAllTags().SerializeToString()

    def _performQuery(self, aggregateName, queryItem):
        registryItem = self.GetRegistryItem(aggregateName)
        aggregates = registryItem.process.Query(queryItem)
        response = ChronosQueryResponse()
        for aggregate in aggregates:
            self._addToQueryResponse(response, aggregate)

        self.statisticsLogger.LogQuery(aggregateName)
        return response.SerializeToString()

    def Shutdown(self):
        """Shuts down all registered ChronosProcessManager and ChronosProcess instances."""
        EventLogger.LogInformationAuto(self, 'Shutting down')
        self.statisticsLogger.Shutdown()
        with self.registryLock:
            for aggregateName, registryItem in self.registry.iteritems():
                try:
                    self.synchronizationManager.DeleteSynchronizationProxy(aggregateName)
                    registryItem.Shutdown()
                except Exception:
                    EventLogger.LogExceptionAuto(self, 'Error shutting down single registry item')

    @staticmethod
    def _addToQueryResponse(response, aggregate):
        proto = response.aggregates.add() #pylint: disable=E1101
        proto.aggregateId = aggregate.aggregateId
        proto.version = aggregate.version - 1
        proto.proto = aggregate.proto.SerializeToString()

if __name__ == '__main__':
    EventLogger.InitializeLogger(application='ChronosGateway')

    EventLogger.LogInformation('Chronos', 'Gateway', 'main', 'Chronos Hard Start')
    INFRASTRUCTUREPROVIDER = InfrastructureProvider()
    SERVICE = INFRASTRUCTUREPROVIDER.GetConfigurablePlugin(ConfigurablePlugin.TransportLayer, transportType=TransportType.Service)
    SERVICE.ProvisionOnStart()
    SERVICE.BlockingRunService(sys.argv)
    SERVICE.CleanupOnExit()
    EventLogger.LogInformation('Chronos', 'Gateway', 'main', 'Chronos Hard Stop')
