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
from redis import StrictRedis, exceptions as RedisExceptions
from threading import Lock, Thread, Event

from Chronos.Chronos_pb2 import ChronosRegistrationResponse, ChronosManagementNotification, ChronosQueryResponse

from Chronos.Core import AggregateLogicCompiler, ValidationError, ChronosFailedEventPersistenceException
from Chronos.Dependency import ChronosProcessSynchronizer
from Chronos.EventLogger import EventLogger
from Chronos.Infrastructure import InfrastructureProvider, ConfigurablePlugin, TransportType, AbstractCoreProvider
from Chronos.PostgresBackend import PostgresLogicConnection, PostgresEventReadConnection, PostgresEventWriteConnection
from Chronos.Profiling import ProcessAwareProfiler


class ChronosGatewayException(Exception):
    """An Exception indicating that Chronos has encountered an error from
    which it is unable to recover. This Exception could be thrown due to
    network events, logic errors, or inconsistent state."""
    pass


def GetRedisConnectionDetails(infrastructureProvider):
    redisConfig = infrastructureProvider.GetSection('DefaultRedisImplementation')
    host = redisConfig.pop('host', '127.0.0.1')
    port = redisConfig.pop('port', 6379)
    return host, port, redisConfig

def GetPostgresConnectionDetails(infrastructureProvider):
    config = infrastructureProvider.GetSection('PostgresConnectionDetails')
    user = config.pop('user')
    password = config.pop('password', '')
    database = config.pop('database')
    host = config.pop('host')
    port = config.pop('port', 5432) # use default postgres port if one isn't supplied
    return user, password, database, host, port

class DefaultCoreProvider(AbstractCoreProvider):
    def __init__(self, aggregateClass, **kwargs): #pylint: disable=W0613
        super(DefaultCoreProvider, self).__init__(aggregateClass)
        self.aggregateName = self.aggregateClass.__name__
        postgresUser, postgresPassword, postgresDatabase, postgresHost, postgresPort = GetPostgresConnectionDetails(self.infrastructureProvider)
        redisHost, redisPort, redisConfig = GetRedisConnectionDetails(self.infrastructureProvider)

        try:
            self.redisConnection = StrictRedis(host=redisHost, port=redisPort, **redisConfig)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Failed to initialize Notifier')
            raise
        self._notifier = self.infrastructureProvider.GetConfigurablePlugin(ConfigurablePlugin.Notifier, aggregateClass=self.aggregateClass, #pylint: disable=C0103
                                                                           redisConnection=self.redisConnection)

        self.logicConnection = PostgresLogicConnection(postgresUser, postgresPassword, postgresDatabase, postgresHost, postgresPort)
        self._logicStore = self.infrastructureProvider.GetConfigurablePlugin(ConfigurablePlugin.LogicStore, postgresConnection=self.logicConnection) #pylint: disable=C0103

        self.eventPersisterConnection = PostgresEventWriteConnection(self.aggregateName, postgresUser, postgresPassword, postgresDatabase, postgresHost, postgresPort)
        self._eventPersister = self.infrastructureProvider.GetConfigurablePlugin(ConfigurablePlugin.EventPersister, aggregateClass=self.aggregateClass, #pylint: disable=C0103
                                                                                 notifier=self._notifier, postgresConnection=self.eventPersisterConnection)

        self.eventReaderConnection = PostgresEventReadConnection(self.aggregateName, postgresUser, postgresPassword, postgresDatabase, postgresHost, postgresPort)
        self._eventReader = self.infrastructureProvider.GetConfigurablePlugin(ConfigurablePlugin.EventReader, aggregateClass=self.aggregateClass, #pylint: disable=C0103
                                                                              postgresConnection=self.eventReaderConnection)

    @property
    def notifier(self):
        return self._notifier

    @property
    def logicStore(self):
        return self._logicStore

    @property
    def eventPersister(self):
        return self._eventPersister

    @property
    def eventReader(self):
        return self._eventReader

    def Dispose(self):
        self.notifier.Dispose()
        del self.redisConnection
        self.logicConnection.Dispose()
        self.eventPersisterConnection.Dispose()
        self.eventReaderConnection.Dispose()


ChronosEventQueueItem = namedtuple('ChronosEventQueueItem', ['aggregateId', 'eventType', 'eventProto', 'requestId', 'senderId',
                                                             'receivedTimestamp', 'tag'])
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
    processing, synchronous query handling, and tag management. Query handling is
    separated from the other operations and cannot affect writing. Tags can optionally
    be specified along with specific events (in which case a failing tag creation will
    fail the entire event).

    This class is threadsafe and all exceptions should be logged and handled internally."""
    def __init__(self, aggregateClass, aggregateRegistry, logicMetadata, coreProviderGenerator, profiler):
        self.aggregateClass = aggregateClass
        self.aggregateRegistry = aggregateRegistry
        self.logicMetadata = logicMetadata
        self.coreProviderGenerator = coreProviderGenerator
        self.profiler = profiler
        self.processSynchronizer = ChronosProcessSynchronizer()
        self.lock = Lock()
        self.queue = Queue.Queue()
        self.queryEvent = Event()
        self.queryEvent.set()
        self.isRunning = False
        self.processingThread = None
        self.coreProvider = None
        self.repository = None
        self.processor = None
        self.notifier = None
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
                self.repository = self.coreProvider.GetRepository()
                self.processor = self.coreProvider.GetProcessor()
                self.notifier = self.coreProvider.GetNotifier()

                self.isRunning = True
                self.processingThread = Thread(target=self.EventLoop, name=self.aggregateClass.__name__)
                self.processingThread.start()

            except Exception:
                EventLogger.LogExceptionAuto(self, 'Unable to start Chronos process', tags={'Aggregate': self.aggregateClass.__name__})
                excInfo = sys.exc_info()
                raise ChronosGatewayException, excInfo[1], excInfo[2]

            try:
                managementNotification = ChronosManagementNotification(notificationType=ChronosManagementNotification.REGISTERED) #pylint: disable=E1101
                for attr in sorted(self.aggregateClass.IndexedAttributes):
                    attribute = managementNotification.indexedAttributes.add() #pylint: disable=E1101
                    attribute.attributeName = attr
                    attribute.isCaseInsensitive = attr in self.aggregateClass.NoCaseAttributes

                self.notifier.PublishGatewayManagementNotification(managementNotification)
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Shutting down Chronos process', 'Exception while publishing startup notification',
                                             tags={'Aggregate': self.aggregateClass.__name__})
                self.Shutdown()
                excInfo = sys.exc_info()
                raise ChronosGatewayException, excInfo[1], excInfo[2]


    def Shutdown(self):
        """Cleans up the Chronos Core instances owned by this instance and stops
        the ChronosProcess::EventLoop. The event loop will complete processing of all
        items in the queue before it allows this method to return."""
        EventLogger.LogInformationAuto(self, 'Shutting down Chronos process',
                                       tags={'Aggregate': self.aggregateClass.__name__})
        with self.lock:
            if not self.isRunning:
                return
            try:
                self.isRunning = False
                self.queue.put(None)
                self.processingThread.join()
                self.processingThread = None

                managementNotification = ChronosManagementNotification(notificationType=ChronosManagementNotification.UNREGISTERED) #pylint: disable=E1101
                self.notifier.PublishGatewayManagementNotification(managementNotification)

            except Exception:
                EventLogger.LogExceptionAuto(self, 'Error shutting down Chronos process',
                                             tags={'Aggregate': self.aggregateClass.__name__})
            finally:
                self.coreProvider.Dispose()
                self.coreProvider = None
                self.repository = None
                self.processor = None
                self.notifier = None

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

    def _tryVerifyEventPersistenceCheckpoint(self):
        while self.isRunning:
            try:
                self.processor.TryVerifyEventPersistenceCheckpoint()
                return
            except ChronosFailedEventPersistenceException:
                EventLogger.LogWarningAuto(self, 'Event Persistence Checkpoint Verification failed.', 'Reattempting verification...',
                                           tags={'AggregateName': self.aggregateClass.__name__})
                time.sleep(1)
            except Exception as exc:
                errorMessage = str(exc)
                EventLogger.LogExceptionAuto(self, 'Event Persistence Checkpoint verification failed', errorMessage,
                                             tags={'AggregateName': self.aggregateClass.__name__})

    def EventLoop(self):
        """Event queue processing. This method should always be run in a background
        thread; it will not return until the ChronosProcess instance is shut down."""
        if not self.isRunning:
            raise ChronosGatewayException('Cannot start event loop - process not running')
        EventLogger.LogInformationAuto(self, 'Starting Chronos event loop',
                                       tags={'Aggregate': self.aggregateClass.__name__})

        self._tryVerifyEventPersistenceCheckpoint()

        try:
            self.profiler.Begin()
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Profiling failed')
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
        self.profiler.End()
        EventLogger.LogInformationAuto(self, 'Chronos event loop exiting',
                                       tags={'Aggregate': self.aggregateClass.__name__})

    def Begin(self):
        self.repository.Begin()
        self.processor.Begin()

    def Commit(self):
        self.processor.Commit()
        self.repository.Commit()

    def Rollback(self):
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
                self.processor.FlushPersistenceBuffer(shouldForce=self.queue.empty())
            except ChronosFailedEventPersistenceException:
                EventLogger.LogExceptionAuto(self, 'Event persistence delayed; transaction integrity ensured',
                                             'Loss of connection to database or notification mechanism',
                                             tags={'Aggregate': self.aggregateClass.__name__})
                time.sleep(1)
            finally:
                self.queryEvent.set()
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Critical error - possibly failing event notification',
                                         'Catastrophic failure encountered during single event processing',
                                         tags={'Aggregate': self.aggregateClass.__name__})
            self.Rollback()

    def ProcessEvent(self, eventQueueItem):
        """Determines the corresponding Python class to an eventType sent from
        a client and applies that type's logic to the specified aggregate. This
        method makes use of the AggregateRegistry created by the Chronos Core
        metaclasses at import time."""
        try:
            existingAggregate = self.GetAggregate(eventQueueItem)
            # Deep copy and emplace are used so that a failed validation doesn't leave the aggregate in a bad state
            aggregateCopy = copy.deepcopy(existingAggregate)
            with existingAggregate.lock:
                aggregate, bufferItem = self.ApplyEvent(aggregateCopy, eventQueueItem)

                self.processor.UpsertAggregateSnapshot(aggregate)

                if existingAggregate.version == 1 or aggregate.HasDivergedFrom(existingAggregate):
                    self.processor.ProcessIndexDivergence(aggregate.aggregateId)

                if eventQueueItem.tag is not None:
                    self.processor.ProcessTag(aggregate, self.logicMetadata.logicId, eventQueueItem.tag, long(time.time()))

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
                self.processor.ProcessFailure(eventQueueItem, ex)
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Critical error - failing event notification',
                                             'EventProcessor unable to send failure notification',
                                             tags={'AggregateId': eventQueueItem.aggregateId, 'EventType': eventQueueItem.eventType,
                                                   'Aggregate': self.aggregateClass.__name__, 'Tag': eventQueueItem.tag})
            return False
        finally:
            self.processSynchronizer.ReleaseDependencies()

    def GetAggregate(self, eventQueueItem):
        """Attempts to retrieve the aggregate instance specified by eventQueueItem.

        @throw ChronosCoreException if eventQueueItem specifies a non-existant aggregate instance"""

        if eventQueueItem.aggregateId == 0:
            aggregate = self.repository.Create()
        else:
            aggregate = self.repository.Get(eventQueueItem.aggregateId)

        return aggregate

    def ApplyEvent(self, aggregate, eventQueueItem):
        eventGenerator = self.aggregateRegistry[eventQueueItem.eventType]
        event = eventGenerator(aggregate.version, self.logicMetadata.logicVersion)
        event.proto.ParseFromString(eventQueueItem.eventProto)
        self.processSynchronizer.AcquireDependencies(aggregate, event)

        return self.processor.Process(eventQueueItem, aggregate, self.logicMetadata.logicId, event)

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
                    return self.repository.GetFromIndex(queryItem.indexKeys)
                else:
                    return self.repository.GetAll()
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Error processing single query',
                                             tags={'AggregateId': queryItem.aggregateId,
                                                   'Aggregate': self.aggregateClass.__name__})
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
    def __init__(self, synchronizationManager, coreProviderGenerator, gatewayStore):
        self.synchronizationManager = synchronizationManager
        self.coreProviderGenerator = coreProviderGenerator
        self.logicStore = gatewayStore.logicStore
        self.crossAggregateAccess = gatewayStore.crossAggregateAccess
        self.logicCompiler = AggregateLogicCompiler(self.logicStore)
        self.statisticsLogger = StatisticsLogger()
        self.profiler = ProcessAwareProfiler('chronos.stats')
        self.profiler.Begin()
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
                        logicMetadata, _ = self.logicStore.GetLatestAggregateLogic(module.AggregateClass.__name__)
                        self.registry[moduleName] = ChronosRegistryItem()
                        self.crossAggregateAccess.GetOrCreateMetadata(module.AggregateClass.__name__)
                        self.AddRegistryItem(logicMetadata, module)
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
    def CreateRegistrationResponse(errorMessage='', indexedAttributes=None, caseInsensitiveAttributes=None):
        response = ChronosRegistrationResponse()
        if errorMessage:
            response.responseCode = ChronosRegistrationResponse.FAILURE #pylint: disable=E1101
            response.responseMessage = errorMessage
        else:
            if indexedAttributes is None or caseInsensitiveAttributes is None:
                raise ChronosGatewayException('Cannot create successful registration response without indexedAttributes')
            response.responseCode = ChronosRegistrationResponse.SUCCESS #pylint: disable=E1101
            for attr in sorted(indexedAttributes):
                attribute = response.indexedAttributes.add() #pylint: disable=E1101
                attribute.attributeName = attr
                attribute.isCaseInsensitive = attr in caseInsensitiveAttributes

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
                aggregateClassId = self.crossAggregateAccess.GetOrCreateMetadata(request.aggregateName)
                logicMetadata, module = self.logicCompiler.Compile(aggregateClassId, request.aggregateName, request.aggregateLogic)
                self.AddRegistryItem(logicMetadata, module)
                self.SaveRegistry()

                return self.CreateRegistrationResponse(indexedAttributes=module.AggregateClass.IndexedAttributes,
                                                       caseInsensitiveAttributes=module.AggregateClass.NoCaseAttributes)
            except Exception as exc:
                errorMessage = str(exc)
                EventLogger.LogExceptionAuto(self, 'Aggregate registration failed', errorMessage,
                                             tags={'AggregateName': request.aggregateName})
                return self.CreateRegistrationResponse(errorMessage=errorMessage)

    def AddRegistryItem(self, logicMetadata, module):
        """Creates a new ChronosProcessManager and ChronosProcess for the
        module created from a ChronosRegistrationRequest."""
        EventLogger.LogInformationAuto(self, 'Adding Registry Item', tags={'AggregateName':module.AggregateClass.__name__})
        manager = ChronosProcessManager()
        manager.start()
        process = manager.ChronosProcess(module.AggregateClass, #pylint: disable=E1101
                                         module.AggregateRegistry, logicMetadata, DefaultCoreProvider, self.profiler.GetProfilingInstance())
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
                        self.synchronizationManager.DeleteSynchronizationProxy(aggregateName)
                        registryItem.Shutdown()
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
            return self.CreateRegistrationResponse(indexedAttributes=registryItem.module.AggregateClass.IndexedAttributes,
                                                   caseInsensitiveAttributes=registryItem.module.AggregateClass.NoCaseAttributes)
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
        return self._routeEvent(request.request, request.tag)

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
                                          tagRequest.tag if tagRequest.tag else None)
            events.append(event)
        transaction = ChronosEventQueueTransaction(events)
        registryItem = self.GetRegistryItem(aggregateName)
        registryItem.process.SendEvent(transaction)

        return requestId

    def _routeEvent(self, request, tag=None):
        aggregateName, eventType = request.eventType.split('.')
        registryItem = self.GetRegistryItem(aggregateName)

        with self.requestIdLock:
            requestId = self.requestIdCounter
            self.requestIdCounter += 1
            eventQueueItem = ChronosEventQueueItem(request.aggregateId, eventType,
                                                   request.eventProto, requestId, request.senderId,
                                                   long(time.time() * 1e9), tag)
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

        # TODO(cjackson): use dictionary comprehension once the QueryByIndex proto uses a better dictionary
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
        self.profiler.End()

    @staticmethod
    def _addToQueryResponse(response, aggregate):
        proto = response.aggregates.add() #pylint: disable=E1101
        proto.aggregateId = aggregate.aggregateId
        proto.version = aggregate.version - 1
        proto.proto = aggregate.proto.SerializeToString()

if __name__ == '__main__':
    os.chdir('/var/lib/chronos')
    EventLogger.InitializeLogger(application='ChronosGateway')

    EventLogger.LogInformation('Chronos', 'Gateway', 'main', 'Chronos Hard Start')
    INFRASTRUCTUREPROVIDER = InfrastructureProvider()
    SERVICE = INFRASTRUCTUREPROVIDER.GetConfigurablePlugin(ConfigurablePlugin.TransportLayer, transportType=TransportType.Service)
    SERVICE.ProvisionOnStart()
    SERVICE.BlockingRunService(sys.argv)
    SERVICE.CleanupOnExit()
    EventLogger.LogInformation('Chronos', 'Gateway', 'main', 'Chronos Hard Stop')
