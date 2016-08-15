"""@file Basic infrastructural components for Chronos.

In addition to contracts that are used across multiple different Chronos modules (the persistence items),
this file also specifies the various "customization points" offered by Chronos. These customization points are
modules that are dynamically loaded at runtime depending on the current system configuration.

For more information, see the abstract definitions for the customization points below:

AbstractEventStore
AbstractServiceProxyManager
AbstractClientProxy
AbstractServiceImplementations

For more information about the definition of the configuration file, see config.ini at the root of the repository.
@addtogroup Chronos
@{
"""
import imp
import os
import time
from abc import ABCMeta, abstractmethod, abstractproperty
from collections import namedtuple
from ConfigParser import ConfigParser

from Chronos.EventLogger import EventLogger
from Chronos.Chronos_pb2 import ChronosResponse

class BufferItem(object):
    __metaclass__ = ABCMeta
    def __init__(self):
        self.serializationResult = None

    @abstractmethod
    def Serialize(self):
        pass

SuccessSerializationResult = namedtuple('SuccessSerializationResult', ['serializedResponse', 'serializedEvent', 'aggregateDict',
                                                                       'aggregate', 'requestId'])
class PersistenceBufferItem(BufferItem):
    def __init__(self, aggregateClass, request, aggregate, singleEvent, receivedTimestamp, processedTimestamp):
        super(PersistenceBufferItem, self).__init__()
        self.aggregateClass = aggregateClass
        self.request = request
        self.aggregate = aggregate
        self.event = singleEvent
        self.receivedTimestamp = receivedTimestamp
        self.processedTimestamp = processedTimestamp

    def Serialize(self):
        eventProto = self.event.ToProto(self.receivedTimestamp, self.processedTimestamp)
        serializedEventProto = eventProto.SerializeToString()
        aggregateDict = self.aggregate.ToDict()
        response = ChronosResponse(responseCode=ChronosResponse.SUCCESS, requestId=self.request.requestId, #pylint: disable=E1101
                                   senderId=self.request.senderId)
        response.eventProto.CopyFrom(eventProto) #pylint: disable=E1101
        response.aggregateProto.aggregateId = self.aggregate.aggregateId #pylint: disable=E1101
        response.aggregateProto.proto = aggregateDict['proto'] #pylint: disable=E1101
        response.aggregateProto.version = self.aggregate.version #pylint: disable=E1101
        return SuccessSerializationResult(response.SerializeToString(), serializedEventProto, aggregateDict, self.aggregate, self.request.requestId)

FailureSerializationResult = namedtuple('FailureSerializationResult', ['serializedResponse'])
class PersistenceBufferFailureItem(BufferItem):
    def __init__(self, aggregateClass, request, exception):
        super(PersistenceBufferFailureItem, self).__init__()
        self.aggregateClass = aggregateClass
        self.request = request
        self.exception = exception

    def Serialize(self):
        response = ChronosResponse(responseCode=ChronosResponse.FAILURE, responseMessage=str(self.exception), #pylint: disable=E1101
                                   requestId=self.request.requestId,
                                   senderId=self.request.senderId)
        response.aggregateProto.aggregateId = self.request.aggregateId #pylint: disable=E1101
        response.eventProto.type = '{0}.{1}'.format(self.aggregateClass.__name__, self.request.eventType) #pylint: disable=E1101
        response.eventProto.proto = self.request.eventProto #pylint: disable=E1101
        response.eventProto.receivedTimestamp = self.request.receivedTimestamp #pylint: disable=E1101
        response.eventProto.processedTimestamp = long(time.time() * 1e9) #pylint: disable=E1101

        if hasattr(self.exception, 'tags') and isinstance(self.exception.tags, dict):
            for key, value in self.exception.tags.iteritems():
                response.additionalInformation.add(key=key, value=str(value)) #pylint: disable=E1101

        return FailureSerializationResult(response.SerializeToString())

ManagementSerializationResult = namedtuple('ManagementSerializationResult', ['serializedNotification'])
class PersistenceBufferManagementItem(BufferItem):
    def __init__(self, aggregateClass, managementNotification):
        super(PersistenceBufferManagementItem, self).__init__()
        self.aggregateClass = aggregateClass
        self.managementNotification = managementNotification

    def Serialize(self):
        return ManagementSerializationResult(self.managementNotification.SerializeToString())

TagSerializationResult = namedtuple('TagSerializationResult', ['tag', 'aggregateDict'])
class PersistenceBufferTagItem(BufferItem):
    def __init__(self, aggregateClass, aggregate, tag, tagExpiration, createDate):
        super(PersistenceBufferTagItem, self).__init__()
        self.aggregateClass = aggregateClass
        self.aggregate = aggregate
        self.tag = tag
        self.tagExpiration = tagExpiration
        self.createDate = createDate

    def Serialize(self):
        aggregateDict = self.aggregate.ToDict()
        aggregateDict['tagExpiration'] = self.tagExpiration
        aggregateDict['createDate'] = self.createDate
        return TagSerializationResult(self.tag, aggregateDict)

class EventPersistenceCheckpoint(object):
    def __init__(self, maxRequestId, aggregateIds):
        self.maxRequestId = maxRequestId
        self.aggregateIds = aggregateIds

    def ShouldPersist(self):
        return self.maxRequestId > 0 and self.aggregateIds

    def Serialize(self):
        return {'maxRequestId': self.maxRequestId,
                'aggregateIds': ','.join(str(aggregateId) for aggregateId in self.aggregateIds)}

    def VerifyCheckpoint(self, indexStore, indexSession, aggregateRepository):
        """Verifies the integrity of indexed data by comparing the previously saved indexed checkpoint to the current data checkpoint.

        @param indexStore An IndexStore instance used to index checkpoint retrieval.
        @param indexSession A SqliteIndex instance used for checkpoint retrieval.
        @param aggregateRepository An AggregateRepository instance used for data checkpoint retrieval.
        """
        indexedCheckpoint = indexStore.GetCheckpoint(indexSession)
        if indexedCheckpoint is not None and indexedCheckpoint.maxRequestId == self.maxRequestId:
            return

        EventLogger.LogWarningAuto(self, 'Augmenting existing index', 'Indexed checkpoint could not be found or requestId diverged',
                                   tags={'Aggregate': indexStore.aggregateClass.__name__})
        for aggregateId in self.aggregateIds:
            aggregate = aggregateRepository.Get(aggregateId)
            try:
                indexSession.Begin()
                indexStore.ReindexAggregate(aggregate, indexSession)
                indexSession.Commit()
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Reindexing failed while rebuilding from persistence checkpoint',
                                             tags={'Aggregate': indexStore.aggregateClass.__name__,
                                                   'AggregateId': aggregateId})
                indexSession.Rollback()
        try:
            indexSession.Begin()
            indexStore.UpdateCheckpoint(self.maxRequestId, indexSession)
            indexSession.Commit()
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Failed to update index checkpoint',
                                         tags={'Aggregate': indexStore.aggregateClass.__name__,
                                               'MaxRequestId': self.maxRequestId})
            indexSession.Rollback()
        indexSession.Commit()
        indexSession.Close()

    @staticmethod
    def Deserialize(dct):
        return EventPersistenceCheckpoint(long(dct['maxRequestId']),
                                          [long(aggregateId) for aggregateId in dct['aggregateIds'].split(',')])

# TODO(jkaye): All of the abstract classes below need to be fully documented (pull documentation from BTImplementations)

class AbstractServiceImplementations(object):
    @abstractmethod
    def __init__(self, infrastructureProvider):
        """
        The main purpose of this class (along with AbstractClientProxy) is to decouple ChronosES from a specific transport implementation.
        Belvedere, for example, uses a CORBA-based RPC transport that most users would not be interested in adopting.
        Implementations of this class are really nothing more than thin wrappers that should connect to some network
        resource and forward requests to Chronos::Gateway::ChronosGateway.

        All methods that you'll need to implement for a different transport are listed below. The easiest way to get
        started with writing your own transport layer plugin would be to take a look at RestServiceImplementation.py,
        understand that code, and then move forward from there.

        @abstractmethod
        def RegisterAggregate(self, request):
            pass

        @abstractmethod
        def UnregisterAggregate(self, aggregateName):
            pass

        @abstractmethod
        def CheckStatus(self, aggregateName):
            pass

        @abstractmethod
        def ProcessEvent(self, request):
            pass

        @abstractmethod
        def ProcessEventWithTag(self, request):
            pass

        @abstractmethod
        def ProcessTransaction(self, request):
            pass

        @abstractmethod
        def GetAll(self, request):
            pass

        @abstractmethod
        def GetById(self, request):
            pass

        @abstractmethod
        def GetByIndex(self, request):
            pass

        @abstractmethod
        def GetByTag(self, request):
            pass

        @abstractmethod
        def GetTags(self, aggregateName):
            pass

        """
        pass

    @abstractmethod
    def ProvisionOnStart(self):
        """Perform any necessary infrastructural setup.
        This method will be called exactly once, after the service is started, but before BlockingRunService.
        """
        pass

    @abstractmethod
    def BlockingRunService(self, commandLineArgs):
        """Perform any necessary actions to finalize running of your transport.
        This method should block until some signal (ie. SIGKILL) is received.
        """
        pass

    @abstractmethod
    def CleanupOnExit(self):
        """Perform any necessary actions to clean up your transport prior to the process exiting.
        This method will be called exactly once, after BlockingRunService yields control.
        """
        pass


class AbstractClientProxy(object):
    __metaclass__ = ABCMeta
    def __init__(self, infrastructureProvider):
        """The main purpose of this class (along with AbstractServiceImplementations) is to decouple ChronosES from a specific transport implementation.
        Please see the documentation of AbstractServiceImplementations for full details about this abstraction.
        All required client method implementations are documented below.

        Any of the methods of this class should be expected to throw arbitrary Exceptions depending on the transport layer implementation being
        used by the client/service pair. Because the transport layer is configurable, there isn't a good way to restrict these Exceptions.
        When implementing an AbstractClientProxy, please make sure to be aware of this caveat and design an appropriate Exception hierarchy for
        your use case.

        All of the below methods should return the raw (serialized) Protobuf format of the specified types; the Chronos::Client::ChronosClient
        handles all necessary parsing.
        """
        pass

    @abstractmethod
    def RegisterAggregate(self, chronosRequest):
        """Registers an Aggregate's data model and logic with the ChronosES service.
        Under normal circumstances (ie. no network issues and service is running), this method should never throw an Exception. Instead,
        the returned ChronosRegistrationResponse will contain success/failure information along with any necessary details.

        @param chronosRequest A Chronos::Chronos_pb2::ChronosRegistrationRequest instance specifying the Aggregate to be registered.
        @returns A Chronos::Chronos_pb2::ChronosRegistrationResponse object detailing the result of the registration request.
        """
        pass

    @abstractmethod
    def UnregisterAggregate(self, aggregateName):
        """Unregisters an Aggregate with the ChronosES service.
        After unregistration, Events will no longer be able to be raised against the Aggregate until a later call
        is made to RegisterAggregate.

        @param aggregateName A string specifying the name of the Aggregate to unregister.
        @returns None
        """
        pass

    @abstractmethod
    def CheckStatus(self, aggregateName):
        """Returns the current registration status of an Aggregate.

        @param aggregateName A string specifying the name of the Aggregate to check.
        @returns ChronosRegistrationResponse
        """
        pass

    @abstractmethod
    def ProcessEvent(self, chronosRequest):
        """Enqueues an Event for future processing on an Aggregate instance.
        If the aggregateId field of @p chronosRequest is set to 0, a new Aggregate instance will be created and the Event
        will be applied against the new instance. Otherwise, the Event will be applied to the instance specified by
        that aggregateId.

        @param chronosRequest A Chronos::Chronos_pb2::ChronosRequest instance specifying the Event to be processed.
        @returns A long representing the requestId for the Event.
        """
        pass

    @abstractmethod
    def ProcessEventWithTag(self, chronosRequest):
        """Identical to AbstractClientProxy::ProcessEvent, except that the resulting Aggregate instance's state will be tagged with the provided tag.

        @param chronosRequest A Chronos::Chronos_pb2::ChronosRequestWithTag instance specifying the Event to be processed and the tag to be used.
        @returns A long representing the requestId for the Event.
        """
        pass

    @abstractmethod
    def ProcessTransaction(self, chronosRequest):
        """Atomically enqueues multiple Events for future processing on Aggregate instances.
        All supplied Events will be processed as an atomic transaction; if any Event in the transaction fails, then none of the Events
        will be applied.

        @param chronosRequest A Chronos::Chronos_pb2::ChronosTransactionRequest instance specifying the Events to be processed.
        @returns A long representing the requestId for the transaction.
        """
        pass

    @abstractmethod
    def GetAll(self, chronosRequest):
        """Returns all Aggregate instance snapshots.

        @param chronosRequest A Chronos::Chronos_pb2::ChronosQueryAllRequest instance.
        @returns A Chronos::Chronos_pb2::ChronosQueryResponse instance.
        """
        pass

    @abstractmethod
    def GetById(self, chronosRequest):
        """Returns the Aggregate instance snapshot for the specified aggregateId.

        @param chronosRequest A Chronos::Chronos_pb2::ChronosQueryByIdRequest instance.
        @returns A Chronos::Chronos_pb2::ChronosQueryResponse instance.
        """
        pass

    @abstractmethod
    def GetByIndex(self, chronosRequest):
        """Returns the Aggregate instance snapshots that satisfy the specified indicies.

        @param chronosRequest A Chronos::Chronos_pb2::ChronosQueryByIndexRequest instance.
        The indexKeys property of @p chronosRequest should be a list of alternating key value pairs.
        @returns A Chronos::Chronos_pb2::ChronosQueryResponse instance.
        """
        pass

    @abstractmethod
    def GetByTag(self, chronosRequest):
        """Returns the Aggregate instance snapshot for the specified tag.

        @param chronosRequest A Chronos::Chronos_pb2::ChronosQueryByTagRequest instance.
        @returns A Chronos::Chronos_pb2::ChronosQueryResponse instance.
        """
        pass

    @abstractmethod
    def GetTags(self, aggregateName):
        """Returns all tags for the specified Aggregate instance.

        @param aggregateName A string specifying which Aggregate to query.
        @returns A Chronos::Chronos_pb2::ChronosTagList instance.
        """
        pass

class AbstractEventKeys(object):
    __metaclass__ = ABCMeta
    def __init__(self, aggregateClass):
        """Initializes an AbstractEventKeys instance that can provide key information for an Aggregate.

        @param aggregateClass The class of the Aggregate for which key information will be provided.
        """
        pass

    @abstractmethod
    def GetEventKey(self, aggregateId):
        """Returns the key that can be used to query Events for an Aggregate instance.

        @param aggregateId A long specifying the Aggregate instance.
        @returns A string representing the key to the Event data.
        """
        pass

    @abstractmethod
    def GetSnapshotKey(self, aggregateId):
        """Returns the key that can be used to query the snapshot for an Aggregate instance.

        @param aggregateId A long specifying the Aggregate instance.
        @returns A string representing the key to the snapshot data.
        """
        pass

    @abstractmethod
    def GetTagWildcard(self):
        """Returns the key that can be used to query tags for an Aggregate instance.

        @returns A string representing the wildcard key to the tag data.
        """
        pass

    @abstractmethod
    def GetTagKey(self, aggregateId, tag):
        """Returns the key that can be used to query a tag for an Aggregate instance.

        @param aggregateId A long specifying the Aggregate instance.
        @param tag A string specifying the tag.
        @returns A string representing the key to the tag snapshot data.
        """
        pass

    @abstractmethod
    def GetPublishingTopic(self, aggregate):
        """Returns the key that can be used to publish/subscribe to updates for an Aggregate instance.

        @param aggregate A Chronos::Core::Aggregate subclass instance.
        @returns A string representing the key to the pub/sub channel.
        """
        pass

    @abstractmethod
    def GetFailureTopic(self):
        """Returns the key that can be used to publish/subscribe to updates Aggregate Event processing failures.

        @returns A string representing the key to the failure pub/sub channel.
        """
        pass

    @abstractmethod
    def GetManagementTopic(self):
        """Returns the key that can be used to publish/subscribe to updates Aggregate management events.

        @returns A string representing the key to the management pub/sub channel.
        """
        pass

class AbstractEventStore(object):
    """An abstract base class detailing the interface required for a ChronosES persistence backend.
    An event store implementation has two primary reponsibilities:
    1. Persistence of Aggregate snapshot/Event data
    2. Notification of processed Events/changes to Aggregate instance state.

    The ChronosES implementation places some constraints on certain methods of this class; these will be documented
    inline for each relevant method.
    """
    __metaclass__ = ABCMeta
    def __init__(self, infrastructureProvider):
        """All AbstractEventStore subclass must implement an __init__ that at least satisfies this signature.
        The Chronos::Gateway::ChronosGateway will pass its InfrastructureProvider instance
        into InfrastructureProvider::GetConfiguredEventStore as a means of event store configuration should it become necessary.
        """
        pass

    @abstractproperty
    def KeyGenerator(self): #pylint: disable=C0103
        """Returns the constructor for the AbstractEventKeys subclass associated with a particular AbstractEventStore implementation.
        """
        pass

    @abstractmethod
    def Dispose(self):
        """Performs all necessary cleanup of remote resources.
        """
        pass

    @abstractmethod
    def GetAggregateId(self, aggregateClass):
        """Returns the current (unused) aggregateId for an Aggregate.
        This value would be the equivalent of the next primary key that would be assigned by a SQL insert.
        This operation should not modify the underlying data store in any way.

        @param aggregateClass A Chronos::Core::Aggregate subclass specifying which Aggregate to query.
        @returns A long.
        """
        pass

    @abstractmethod
    def GetAndIncrementAggregateId(self, aggregateClass):
        """Increments and returns the current aggregateId for an Aggregate.
        This value would be the equivalent of the next primary key that would be assigned by a SQL insert.
        This operation should permanently modify the stored aggregateId in the underlying data store.

        @param aggregateClass A Chronos::Core::Aggregate subclass specifying which Aggregate to query.
        @returns A long.
        """
        pass

    @abstractmethod
    def PersistAggregateLogic(self, aggregateClass, aggregateLogic):
        """Persists new Aggregate logic into the underlying data store and returns the new logicId.
        The logicId is a value representing the "primary key" for the logic in the underlying data store.
        This value is used during Event replay to determine which "version" of logic a given Event was run with.

        @param aggregateLogic A Chronos::Chronos_pb2::AggregateLogic instance to be persisted.
        @returns A long.
        """
        pass

    @abstractmethod
    def GetLatestAggregateLogic(self, aggregateClass):
        """Return the most recently registered version of an Aggregate's logic/data model.

        @param aggregateClass A Chronos::Core::Aggregate subclass specifying which Aggregate to query.
        @returns A dictionary of logicId to the Chronos::Chronos_pb2::AggregateLogic instance representing the logic, or None if no logic could be found.
        """
        pass

    @abstractmethod
    def GetLatestAggregateLogicByName(self, aggregateName):
        """Return the most recently registered version of an Aggregate's logic/data model.

        @param aggregateName The name of the Aggregate for which logic should be retrieved.
        @returns A Chronos::Chronos_pb2::AggregateLogic instance representing the logic
        """
        pass

    @abstractmethod
    def GetAggregateLogic(self, aggregateClass, logicIds):
        """Returns multiple versions of logic for an Aggregate.

        @param aggregateClass A Chronos::Core::Aggregate subclass specifying which Aggregate to query.
        @param logicIds A list of longs specifying which logic versions should be retrieved.
        @returns A dictionary of logicId to the Chronos::Chronos_pb2::AggregateLogic instance representing the logic.
        """
        pass

    @abstractmethod
    def PersistEvents(self, aggregateClass, events):
        """Atomically persist Events to the underlying data store and notify subscribed clients of the modifications.
        Until this method is called, an Event has not truly been applied. Your implementation must ensure that data persistence
        and notification is atomic; otherwise, it's possible for Events to be applied without notifying clients, breaking one
        of the ChronosES guarantees.

        The input to this method is a list of BufferItem subclass instances (PersistenceBufferItem, PersistenceBufferFailureItem, etc).
        The method implementation must handle ALL possible BufferItem types and persist/notify clients of them accordingly.

        @see Chronos::DefaultImplementations for a reference implementation using Redis.

        @param aggregateClass A Chronos::Core::Aggregate subclass specifying which Aggregate to query.
        @param events A list of BufferItem subclasses to be persisted.
        """
        pass

    @abstractmethod
    def PublishManagementNotification(self, aggregateClass, notification):
        """Publish a notification the subscribed clients that a management event has occured.

        @param aggregateClass A Chronos::Core::Aggregate subclass specifying which Aggregate to query.
        @param notification A Chronos::Chronos_pb2::ChronosManagementNotification instance.
        """
        pass

    @abstractmethod
    def GetEventPersistenceCheckpoint(self, aggregateClass):
        """Returns the current EventPersistenceCheckpoint for an Aggregate.

        @param aggregateClass A Chronos::Core::Aggregate subclass specifying which Aggregate to query.
        @returns An EventPersistenceCheckpoint instance.
        """
        pass

    @abstractmethod
    def GetEventsToVersion(self, aggregate, toVersion):
        """Returns Events for an Aggregate instance from a lower version to a higher one.

        @param aggregate A Chronos::Core::Aggregate subclass instance. The aggregateId and version from this instance are used.
        @param toVersion A long specifying the version to which Events should be retrieved.
        @returns A list of Chronos::Core::Event subclass instances.
        """
        pass

    @abstractmethod
    def GetEventsByTimestampRange(self, aggregate, fromTimestamp, toTimestamp):
        """Returns Events for an Aggregate instance from one point in time to another.

        @param aggregate A Chronos::Core::Aggregate subclass instance. The aggregateId from this instance is used.
        @param fromTimestamp A long specifying the start time in nanosecond-precision Epoch time.
        @param toTimestamp A long specifying the end time in nanosecond-precision Epoch time.
        @returns A list of Chronos::Core::Event subclass instances.
        """
        pass

    @abstractmethod
    def TryGetSnapshot(self, aggregateClass, aggregateId):
        """Returns the latest snapshot of an Aggregate instance's state.

        @param aggregateClass A Chronos::Core::Aggregate subclass specifying which Aggregate to query.
        @param aggregateId A long specifying the Aggregate instance to query.
        @returns A Chronos::Core::Aggregate subclass instance with the latest snapshot state, or None if none could be found.
        """
        pass

    @abstractmethod
    def GetAllSnapshots(self, aggregateClass):
        """Returns the latest snapshot of all Aggregate instance states.

        @param aggregateClass A Chronos::Core::Aggregate subclass specifying which Aggregate to query.
        @returns A list of Chronos::Core::Aggregate subclass instances.
        """
        pass

    @abstractmethod
    def GetIndexedSnapshots(self, aggregateClass, aggregateIds):
        """Returns the latest snapshot of a subset of Aggregate instance states.

        @param aggregateClass A Chronos::Core::Aggregate subclass specifying which Aggregate to query.
        @param aggregateIds A list of longs specifying the aggregateIds to query.
        @returns A list of Chronos::Core::Aggregate subclass instances.
        """
        pass

    @abstractmethod
    def GetTags(self, aggregateClass):
        # TODO(jkaye): This has to be fixed; requires an aggregateId now since we changed the tag concept.
        pass

    @abstractmethod
    def GetTag(self, aggregateClass, aggregateId, tag):
        """Returns an Aggregate instance snapshot at a tagged point in time.

        @param aggregateClass A Chronos::Core::Aggregate subclass specifying which Aggregate to query.
        @param aggregateId A long specifying which Aggregate instance to query.
        @param tag A string specifying the tag to retrieve.
        @returns A Chronos::Core::Aggregate subclass instance, or None if the tag could not be found.
        """
        pass

    @abstractmethod
    def GetAllAggregateNames(self):
        """Returns the names of all Aggregates that have ever registered with ChronosES.

        @returns A list of strings specifying the Aggregate names.
        """
        pass

    @abstractmethod
    def Subscribe(self, prefix, channel, callback):
        # TODO(jkaye): Fix this method signature. Prefix shouldn't be leaking here.
        """Subscribes to notifications for Event application updates.

        @param prefix Unused
        @param channel A string specifying the topic to which the event store should subscribe.
        @param callback A function to be called whenever a notification is received.
        """
        pass

    @abstractmethod
    def Unsubscribe(self, prefix, channel):
        # TODO(jkaye): Fix this method signature. Prefix shouldn't be leaking here.
        """Unsubscribes to notifications for Event application updates.

        @param prefix Unused
        @param channel A string specifying the topic to which the event store should unsubscribe.
        """
        pass

class AbstractServiceProxyManager(object):
    __metaclass__ = ABCMeta

    def __init__(self, infrastructureProvider):
        self.infrastructureProvider = infrastructureProvider

    @abstractmethod
    def Dispose(self):
        pass

    @abstractmethod
    def Connect(self):
        """Returns AbstractClientProxy for chronos.
        """
        pass

    @abstractmethod
    def Disconnect(self):
        pass

class TransportType(object):
    Client = 'client'
    Service = 'service'

class ConfigurablePlugin(object):
    EventStore = 'EventStore'
    TransportLayer = 'TransportLayer'
    ServiceProxyManager = 'ServiceProxyManager'

def ProcessConfiguredModule(provider, filePath, className, abstractType, **kwargs):
    fileName = os.path.basename(filePath)
    moduleName, _ = os.path.splitext(fileName)
    configuredClass = getattr(imp.load_source(moduleName, filePath), className)
    if not issubclass(configuredClass, abstractType):
        raise ValueError('Configured class {0}.{1} is not an implementation of {2}'.format(moduleName, className, abstractType))
    return configuredClass(infrastructureProvider=provider, **kwargs)

def ProcessConfiguredEventStore(provider, filePath, className, **kwargs):
    return ProcessConfiguredModule(provider, filePath, className, AbstractEventStore, **kwargs)

def ProcessConfiguredTransportLayer(provider, filePath, serviceClassName, clientClassName, transportType, **kwargs):
    if transportType == TransportType.Client:
        return ProcessConfiguredModule(provider, filePath, clientClassName, AbstractClientProxy, **kwargs)
    elif transportType == TransportType.Service:
        return ProcessConfiguredModule(provider, filePath, serviceClassName, AbstractServiceImplementations, **kwargs)
    else:
        raise Exception('Please provide either service or client type')

def ProcessConfiguredServiceProxyManager(provider, filePath, className, **kwargs):
    return ProcessConfiguredModule(provider, filePath, className, AbstractServiceProxyManager, **kwargs)

class InfrastructureProvider(object):
    """Parses an optional configuration file to determine which "pluggable" components should be used by an instance of the Chronos service.
    The file should have the following format. All sections are optional, but if a section is supplied, it should be fully specified.

    [EventStore]
    filePath=<path>
    className=<name>

    [TransportLayer]
    filePath=<path>
    serviceClassName=<name>
    clientClassName=<name>

    [ServiceProxyManager]
    filePath=<path>
    className=<name>
    """
    ConfigFilePath = '/etc/chronos/config.ini'
    ConfigurablePluginStructure = {ConfigurablePlugin.EventStore: {'filePath':  os.path.join(os.path.dirname(__file__), 'DefaultImplementations.py'),
                                                                   'className': 'RedisEventStore'},
                                   ConfigurablePlugin.TransportLayer: {'filePath': os.path.join(os.path.dirname(__file__), 'RestServiceImplementation.py'),
                                                                       'serviceClassName': 'ChronosGatewayRestService',
                                                                       'clientClassName': 'ChronosGatewayRestClient'},
                                   ConfigurablePlugin.ServiceProxyManager: {'filePath': os.path.join(os.path.dirname(__file__), 'DefaultImplementations.py'),
                                                                            'className': 'ServiceProxyManager'}}
    ConfigurablePluginProcessors = {ConfigurablePlugin.EventStore: ProcessConfiguredEventStore,
                                    ConfigurablePlugin.TransportLayer: ProcessConfiguredTransportLayer,
                                    ConfigurablePlugin.ServiceProxyManager: ProcessConfiguredServiceProxyManager}

    def __init__(self):
        """Instantiates a new InfrastructureProvider instance.

        @throws EnvironmentError If a configuration file exists, but could not be read (or was otherwise invalid).
        """
        self.config = None
        configFilePath = os.getenv('CHRONOS_CONFIG_FILE', self.ConfigFilePath)
        EventLogger.LogInformationAuto(self, 'Attempting to load configuration file',
                                       tags={'FilePath': configFilePath})
        if os.path.isfile(configFilePath):
            EventLogger.LogInformationAuto(self, 'Parsing configured overrides', 'Configuration file found')
            self.config = ConfigParser()
            if not self.config.read(self.ConfigFilePath):
                raise EnvironmentError('Unable to read configuration from {0}'.format(self.ConfigFilePath))

    def OverridesExist(self):
        """Returns a boolean representing whether or not a configuration file was found.
        """
        return self.config is not None

    def GetSection(self, section):
        """Returns the data for a configuration section as a dictionary.

        @param section A string specifying the configuration section to be read.
        @returns A (possibly empty) dictionary. The dictionary will be empty if the configuration section is empty or if no configuration was found.
        """
        data = {}
        if self.OverridesExist() and self.config.has_section(section):
            data = dict(self.config.items(section))
        return data

    def GetConfigurablePlugin(self, pluginType, **kwargs):
        try:
            pluginStructure = self.ConfigurablePluginStructure[pluginType]
            pluginProcessor = self.ConfigurablePluginProcessors[pluginType]
        except KeyError:
            raise KeyError('Unknown pluginType {0}'.format(pluginType))
        conflictingKeys = [key for key in kwargs.iterkeys() if key in pluginStructure]
        if conflictingKeys:
            raise ValueError('Received conflicting keyword argument(s) {0}. This conflicts with part of the {1} plugin structure.'.format(conflictingKeys, pluginType))

        config = pluginStructure.copy()
        config.update(kwargs)
        if self.OverridesExist() and self.config.has_section(pluginType):
            for key in pluginStructure.iterkeys():
                config[key] = self.config.get(pluginType, key)
                EventLogger.LogInformationAuto(self, 'Using overridden configurable implementation', 'Found override section',
                                               tags=config)
        return pluginProcessor(self, **config)
