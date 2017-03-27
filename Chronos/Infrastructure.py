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
from abc import ABCMeta, abstractmethod, abstractproperty
from ConfigParser import ConfigParser

from Chronos.EventLogger import EventLogger
from Chronos.Core import AggregateRepository, EventProcessor


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


class AbstractCoreProvider(object):
    """A factory class that provides implementations of core Chronos functionality to other modules. This class is configurable to allow arbitrary parameters to be
    passed to the configurable plugins. Any resources that need to be cleaned up should be created and disposed here rather than in the individual plugins.

    @see Chronos::Gateway"""
    __metaclass__ = ABCMeta
    def __init__(self, aggregateClass):
        """The init is responsible for creating each of the configurable plugins that the ChronosProcess uses (Notifier, LogicStore, EventPersister, and EventReader).
        """
        self.infrastructureProvider = InfrastructureProvider()
        self.aggregateClass = aggregateClass
        self.aggregateRepository = None
        self.eventProcessor = None

    @abstractproperty
    def notifier(self):
        pass

    @abstractproperty
    def logicStore(self):
        pass

    @abstractproperty
    def eventPersister(self):
        pass

    @abstractproperty
    def eventReader(self):
        pass

    @abstractmethod
    def Dispose(self):
        """Disposes all resources that are used by the configurable plugins.
        """
        pass

    def GetNotifier(self):
        return self.notifier

    def GetRepository(self):
        if self.aggregateRepository is None:
            self.aggregateRepository = AggregateRepository(self.aggregateClass, self.eventReader)
        return self.aggregateRepository

    def GetProcessor(self):
        if self.eventProcessor is None:
            self.eventProcessor = EventProcessor(self.aggregateClass, self.eventPersister)
        return self.eventProcessor


class AbstractGatewayStore(object):
    """A factory class that provides access to the LogicStore and CrossAggregateAccess for the ChronosGateway. These plugins must be created separately from
    the ones in AbstractCoreProvider because they are not associated with a particular aggregateClass.
    """
    __metaclass__ = ABCMeta
    def __init__(self, infrastructureProvider):
        self.infrastructureProvider = infrastructureProvider

    @abstractproperty
    def logicStore(self):
        pass

    @abstractproperty
    def crossAggregateAccess(self):
        pass

    @abstractmethod
    def Dispose(self):
        pass


class AbstractEventPersistenceCheckpoint(object):
    """An abstract base class detailing the interface required for a ChronosES persistence checkpoint. The persistence checkpoint's
    primary responsibility is to ensure that data persistence (in the AbstractEventStore) and notification (in the Abstract Notifier) are eventually consistent;
    otherwise, it's possible for Events to be applied without notifying clients, breaking one of the ChronosES guarantees.
    """
    __metaclass__ = ABCMeta
    def __init__(self, maxRequestId):
        """@param maxRequestId An integer indicating the Id of the most recently setn notification. It indicate that the event corresponding to it and all events
        corresponding to requestIds less than it have been successfully notified.
        """
        self.maxRequestId = maxRequestId

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.maxRequestId == other.maxRequestId
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @abstractmethod
    def ShouldPersist(self):
        """Ensures that an Event Persistence Checkpoint is in a valid state and ready for persistence."""
        pass

    @abstractmethod
    def Serialize(self):
        """Serializes the Event Persistence Checkpoint."""
        pass

    @abstractmethod
    def Deserialize(self):
        """Deserializes the Event Persistence Checkpoint."""
        pass


class AbstractNotifier(object):
    """An abstract base class detailing the interface required for a ChronosES notification manager. A Notifier implementation
    has the primary responsibility to notify the client of processed Events/changes to Aggregate instance state.

    The ChronosES implementation places some constraints on certain methods of this class; these will be documented
    inline for each relevant method.
    """

    __metaclass__ = ABCMeta
    def __init__(self, infrastructureProvider, aggregateClass):
        """All AbstractNotifier subclass must implement an __init__ that at least satisfies this signature.

        @param infrastructureProvider The Chronos::Gateway::ChronosGateway will pass its InfrastructureProvider instance
        as a means of notification configuration.
        @param aggregateClass A Chronos::Core::Aggregate subclass specifying which Aggregate to query.
        """
        self.infrastructureProvider = infrastructureProvider
        self.aggregateClass = aggregateClass

    @abstractmethod
    def Dispose(self):
        """Performs all necessary cleanup of remote resources."""
        pass

    @abstractmethod
    def Rollback(self):
        """Performs all necessary cleanup of internal resources."""
        pass

    @abstractmethod
    def PublishPersistedEventNotification(self, aggregateIndices, notification):
        """Publish a notification to the subscribed clients indicating that an event has been persisted to the underlying data store.

        @param aggregateIndices The indices of the aggregate which identify the channel the notification should be sent to.
        @param notification A Chronos::Chronos_pb2::ChronosResponse instance serialized to string format.
        """
        pass

    @abstractmethod
    def PublishGatewayManagementNotification(self, notification):
        """Publish a notification to the subscribed clients that a management event has occured.

        @param notification A Chronos::Chronos_pb2::ChronosManagementNotification instance serialized to string format.
        """
        pass

    @abstractmethod
    def PublishManagementNotification(self, notification):
        """Publish a notification to the subscribed clients that a management event has occured.

        @param notification A Chronos::Chronos_pb2::ChronosManagementNotification instance serialized to string format.
        """
        pass

    @abstractmethod
    def PublishFailureNotification(self, notification):
        """Publish a notification to the subscribed clients that a an event has failed.

        @param notification A Chronos::Chronos_pb2::ChronosResponse instance serialized to string format.
        """

    @abstractmethod
    def BeginTransaction(self):
        """Opens a multi-execution transaction in the notifier. Used to publish responses for multiple events at once without closing the
        transaction until it is committed.
        @throws ChronosTransactionInProgressException If a previous transaction is already opened and BeginTransaction() is called"""
        pass

    def CommitTransaction(self, eventPersistenceCheckpoint):
        """Both closes and executes a transaction that must have been preivously opened with a 'BeginTransaction()' method call. Commiting a transaction must be atomic
        or events may be persisted without being notfied resulting in erroneous state. This method takes in an eventPersistenceCheckpoint representing the last
        notification sent; it is imperative that this event persistence checkpoint be stored atomically in the same transaction.

        @param eventPersistenceCheckpoint A Chronos::Infrastructure::AbstractEventPersistenceCheckpoint representing the last notification sent
        @throws ChronosTransactionException If  transaction is not already opened and 'CommitTransaction()' is called
        """
        pass

    @abstractmethod
    def GetEventPersistenceCheckpoint(self):
        """Retrieves and returns the Chronos::Infrastructure::AbstractEventPersistenceCheckpoint stored in the Notifier.

        @returns The Chronos::Infrastructure::AbstractEventPersistenceCheckpoint stored in the notifier."""
        pass

    @abstractmethod
    def GetSubscriptionChannel(self, indices, indexedAttributes):
        """Returns the Subscription channel for client subscription depending on the available indexedAttributes and the indices the client wants to subscribe to.

        @param indices A dictionary of the indices the client is interested in subscribing to.
        @param indexedAttributes A set of the indexed attributes of the aggregate class.
        @returns A string representing the subscription channel of which a client can subscribe.
        """
        pass

    @abstractmethod
    def Subscribe(self, channel, callback):
        """Subscribes to notifications for Event application updates.

        @param channel A string specifying the topic to which the event store should subscribe.
        @param callback A function to be called whenever a notification is received.
        """
        pass

    @abstractmethod
    def SubscribeForManagement(self, callback):
        """Subscribes to notifications for management updates. (Handles discovering the management channel and calls 'Subscribe()' under the hood.)

        @param callback A callable object used as the notifier's subscription callback for successful Chronos Management messages """
        pass

    @abstractmethod
    def SubscribeForFailure(self, callback):
        """Subscribes to notifications for failed event updates. (Handles discovering the failure channel and calls 'Subscribe()' under the hood.)

        @param callback A callable object used as the notifier's subscription callback for failed Chronos event messages """
        pass

    @abstractmethod
    def Unsubscribe(self, channel):
        """Unsubscribes to notifications for Event application updates.

        @param channel A string specifying the topic to which the event store should unsubscribe.
        """
        pass

    @abstractmethod
    def UnsubscribeForManagement(self):
        """Unsubscribes to notifications for management updates. (Handles discovering the failure channel and calls 'Unsubscribe()' under the hood.)"""
        pass

    @abstractmethod
    def UnsubscribeForFailure(self):
        """Unsubscribes to notifications for failed event updates. (Handles discovering the failure channel and calls 'Unsubscribe()' under the hood.)"""
        pass


class AbstractEventPersister(object):
    """An abstract base class detailing the interface required for a ChronosES persistence backend.
    An event store implementation has the primary reponsibility of persisting Aggregate snapshot/Event data

    The ChronosES implementation places some constraints on certain methods of this class; these will be documented
    inline for each relevant method.
    """
    __metaclass__ = ABCMeta
    def __init__(self, infrastructureProvider, aggregateClass, notifier):
        """All AbstractEventPersister subclass must implement an __init__ that at least satisfies this signature.

        @param infrastructureProvider The Chronos::Gateway::ChronosGateway will pass its InfrastructureProvider instance
        as a means of event store configuration.
        @param aggregateClass A Chronos::Core::Aggregate subclass.
        @param notifier A Chronos::Infrastrucutre::AbstractNotifier subclass specifying an initialized notifier to use.
        """
        self.aggregateName = aggregateClass.__name__
        self.notifier = notifier
        self.infrastructureProvider = infrastructureProvider
        self.transactionSavepointName = 'TransactionSavepoint'

    @abstractmethod
    def Begin(self):
        pass

    @abstractmethod
    def Commit(self):
        pass

    @abstractmethod
    def Rollback(self):
        pass

    @abstractmethod
    def RollbackNotifier(self):
        pass

    @abstractmethod
    def PersistEvents(self, events):
        """Atomically persist Events to the underlying event store. Until this method is called, an Event has not truly been applied.
        Your implementation must ensure that data persistence (in the AbstractEventPersister) and notification (in the AbstractNotifier) is atomic;
        otherwise, it's possible for Events to be applied without notifying clients, breaking one of the ChronosES guarantees.

        The input to this method is a list of SerializationResults (SuccessSerializationResult, FailureSerializationResult, etc).
        The method implementation must handle ALL possible SerializationResult types and persist/notify clients of them accordingly.

        @see Chronos::DefaultImplementations for a reference implementation using PostgreSQL.
        @see Chronos::Core for the definitions of the SerializationResult types.

        @param events A list of SerializationResults.
        """
        pass

    @abstractmethod
    def TryVerifyEventPersistenceCheckpoint(self):
        """The responsibility of this method is to check on startup for all events that have been persisted into the event store, but have yet to be notified and notify
        for them if necessary. This is required to protect against the possibility of an an unexpected shutdown immediately after after event persistence and prior to
        event notification."""
        pass

    @abstractmethod
    def UpsertAggregateSnapshot(self, aggregate):
        """Updates the indices for the given aggregate in the event store. This method is also responsible for creating the aggregate in the event store if
        it does not exist yet. If the index values of the given aggregate invalidate any of the constraints placed upon the aggregate, this method should raise
        an exception and not make changes to the event store.

        @param aggregate A Chronos::Core::Aggregate subclass instance."""
        pass


class AbstractEventReader(object):
    """An abstract base class detailing the interface required for a ChronosES event querier. An AbstractEventReader
    has the primary responsibility of getting the persisted Events/changes.

    The ChronosES implementation places some constraints on certain methods of this class; these will be documented
    inline for each relevant method.
    """

    __metaclass__ = ABCMeta
    def __init__(self, infrastructureProvider, aggregateClass):
        """All AbstractEventReader subclass must implement an __init__ that at least satisfies this signature.

        @param infrastructureProvider The Chronos::Gateway::ChronosGateway will pass its InfrastructureProvider instance
        as a means of event store configuration.
        @param aggregateClass A Chronos::Core::Aggregate subclass specifying which Aggregate to query.
        """
        pass

    @abstractmethod
    def GetNextAggregateId(self):
        """Returns the current (unused) aggregateId for an Aggregate.
        This value would be the equivalent of the next primary key that would be assigned by a SQL insert.
        This operation should not modify the underlying data store in any way.

        @returns A long.
        """
        pass

    @abstractmethod
    def GetEventsToVersion(self, aggregate, toVersion):
        """Returns Events for an Aggregate instance from a lower version to a higher one (inclusive).

        @param aggregate A Chronos::Core::Aggregate subclass instance. The aggregateId and version from this instance are used.
        @param toVersion A long specifying the version to which Events should be retrieved.
        @returns A list of Chronos::Core::Event subclass instances.
        """
        pass

    @abstractmethod
    def GetEventsByTimestampRange(self, aggregate, fromTimestamp, toTimestamp):
        """Returns Events for an Aggregate instance from one point in time to another (inclusive).

        @param aggregate A Chronos::Core::Aggregate subclass instance. The aggregateId from this instance is used.
        @param fromTimestamp A long specifying the start time in nanosecond-precision Epoch time.
        @param toTimestamp A long specifying the end time in nanosecond-precision Epoch time.
        @returns A list of Chronos::Core::Event subclass instances.
        """
        pass

    @abstractmethod
    def TryGetSnapshot(self, aggregateId):
        """Returns the latest snapshot of an Aggregate instance's state.

        @param aggregateId A long specifying the Aggregate instance to query.
        @returns A Chronos::Core::Aggregate subclass instance with the latest snapshot state, or None if none could be found.
        """
        pass

    @abstractmethod
    def GetAllSnapshots(self):
        """Returns the latest snapshot of all Aggregate instance states.

        @returns A list of Chronos::Core::Aggregate subclass instances.
        """
        pass

    @abstractmethod
    def GetIndexedSnapshots(self, indexDict):
        """Returns the latest snapshot of a subset of Aggregate instance states. The indexDict is analogous to specifying columns and values in a SQL WHERE clause.

        @param indexDict A dictionary specifying which index values to query on.
        @returns A list of Chronos::Core::Aggregate subclass instances.
        """
        pass

    @abstractmethod
    def TryGetTag(self, aggregateId, tag):
        """Returns an Aggregate instance snapshot at a tagged point in time.

        @param aggregateId A long specifying which Aggregate instance to query.
        @param tag A string specifying the tag to retrieve.
        @returns A Chronos::Core::Aggregate subclass instance, or None if the tag could not be found.
        """
        pass

    @abstractmethod
    def GetTags(self, aggregateId):
        """Returns a list of all tags associated with the given aggregateId.

        @returns A list of strings specifying all tags on the aggregateId.
        """
        pass

    @abstractmethod
    def RetrieveAggregateIds(self, **indices):
        """Returns a list of all aggregateIds that satisfy the provided index properties.
        This method will raise an Exception if a non-indexed property is supplied as a keyword argument.
        @see AbstractEventReader::GetIndexedSnapshots for an explanation of the indices (there they are passed as a dictionary rather than keyword arguments)

        @param indices Kwargs representing the indexed values that should be used in the lookup.
        @throws ChronosCoreException If the Aggregate is not indexed.
        """
        pass

    @abstractmethod
    def RetrieveAggregates(self, startRow, returnCount, sortField, sortDescending, noCaseAttributes, **indices):
        """Returns a list of all aggregates that satisfy the provided index properties.
        This method will raise an Exception if a non-indexed property is supplied as a keyword argument or sortField.
        @see AbstractEventReader::GetIndexedSnapshots for an explanation of the indices (there they are passed as a dictionary rather than keyword arguments)

        @param startRow An integer specifying the row offset for the query.
        @param returnCount The maximum number of results to return.
        @param sortField An indexed field to sort by or None if sort order does not matter.
        @param sortDescending A boolean dictating if the results should be sorted in descending order. Only used if sortField is specified.
        @param indices Kwargs representing the indexed values that should be used in the lookup.
        @throws ChronosCoreException If the Aggregate is not indexed.
        @returns A list of Chronos::Core::Aggregate subclass instances.
        """
        pass


class AbstractCrossAggregateAccess(object):
    """An abstract base class detailing the interface required for data store access across multiple aggregates.
    The primary responsibility of the AbstractCrossAggregateAccess is to create or retrieve Aggregate information.

    The ChronosES implementation places some constraints on certain methods of this class; these will be documented
    inline for each relevant method.
    """

    __metaclass__ = ABCMeta
    def __init__(self, infrastructureProvider):
        """All AbstractEventReader subclass must implement an __init__ that at least satisfies this signature.

        @param infrastructureProvider The Chronos::Gateway::ChronosGateway will pass its InfrastructureProvider instance
        as a means of configuration.
        """
        pass

    @abstractmethod
    def GetOrCreateMetadata(self, aggregateName):
        """Retrieves the identifier for the Aggregate from the storage backend.
        If no identifier can be found, then an initial structure will be populated into the database and the newly created identifier will be returned.

        @param aggregateName the name of the Aggregate for which the identifier should be retrieved.
        @returns The identifier for the Aggregate.
        @throws Any Exception thrown by the underlying storage connection.
        """
        pass

    @abstractmethod
    def GetAllAggregateNames(self):
        """Returns the names of all Aggregates that have ever registered with ChronosES.

        @returns A list of strings specifying the Aggregate names.
        """
        pass


class AbstractLogicStore(object):
    """An abstract base class detailing the interface required for a ChronosES logic store.
    A logic store implementation has the primary reponsibility of maintaining the logic versions for every aggregateClass.

    The ChronosES implementation places some constraints on certain methods of this class; these will be documented
    inline for each relevant method.
    """
    __metaclass__ = ABCMeta
    def __init__(self, infrastructureProvider):
        """All AbstractLogicStore subclass must implement an __init__ that at least satisfies this signature.

        @param infrastructureProvider The Chronos::Gateway::ChronosGateway will pass its InfrastructureProvider instance
        as a means of configuration.
        """
        pass

    @abstractmethod
    def PersistAggregateLogic(self, aggregateClassId, aggregateName, logicVersion, aggregateLogic, module):
        """Persists new Aggregate logic into the underlying data store and returns the new logicId.
        The logicId is a value representing the "primary key" for the logic in the underlying data store.
        This value is used during Event replay to determine which "version" of logic a given Event was run with.

        @param aggregateClassId A long that identifies the aggregate this logic pertains to.
        @param aggregateName The name of the Aggregate for which this logic pertains to.
        @param logicVersion A long that identifies the logic associated with this aggregate.
        @param aggregateLogic A Chronos::Chronos_pb2::AggregateLogic instance to be persisted.
        @param module A module containing the provided aggregateLogic, wired to the proper Protobuf entities.
        @returns A long.
        """
        pass

    @abstractmethod
    def GetLatestAggregateLogic(self, aggregateName):
        """Returns the most recently registered version of an Aggregate's logic/data model.

        @param aggregateName The name of the Aggregate for which logic should be retrieved.
        @returns A named tuple with logicId and logicVersion, and a Chronos::Chronos_pb2::AggregateLogic instance.
        """
        pass

    @abstractmethod
    def GetPreviousAggregateLogics(self, aggregateName, logicVersions):
        """Returns multiple versions of logic for an Aggregate.

        @param aggregateName The name of the Aggregate for which logic should be retrieved.
        @param logicVersions A list of longs specifying which logic versions should be retrieved.
        @returns A dictionary of logicVersion to the Chronos::Chronos_pb2::AggregateLogic instance representing the logic.
        """
        pass


class AbstractServiceProxyManager(object):
    """An abstract base class describing the interface needed for a discovery service allowing
    the chronos client to find the corresponding service.
    """
    __metaclass__ = ABCMeta

    def __init__(self, infrastructureProvider, clientProxyChangedFunc):
        """All AbstractServiceProxyManager sub classes should call this ctor directly if overriding __init__

        @param infrastructureProvider an instance of InfrastructureProvider providing the ability to lookup how to connect to Chronos Service
        @param clientProxyChangedFunc a function that takes in a client proxy and allows ChronosClient to update its proxy whenever the discovery
               service determines a need for a new proxy
        """
        self.infrastructureProvider = infrastructureProvider
        self.clientProxyChangedFunc = clientProxyChangedFunc

    @abstractmethod
    def Dispose(self):
        """Allows cleanup of resources since the AbstractServiceProxyManager subclass will be owned
        by ChronosClient
        """
        pass

    @abstractmethod
    def Connect(self):
        """Called on ChronosClient construction as well as ChronosClient.Connect to provide the ChronosClient
        with a proxy for its calls to the service

        @returns an AbstractClientProxy subclass instance
        """
        pass

    @abstractmethod
    def Disconnect(self):
        """Disconnects from the discovery service and will not receive updates for new endpoints
        """
        pass


class ConfigurablePlugin(object):
    """An Enum class describing the different types of configurable ChronosES plugins.
    """
    LogicStore = 'LogicStore'
    TransportLayer = 'TransportLayer'
    ServiceProxyManager = 'ServiceProxyManager'
    EventPersister = 'EventPersister'
    EventReader = 'EventReader'
    Notifier = 'Notifier'
    CrossAggregateAccess = 'CrossAggregateAccess'
    CoreProvider = 'CoreProvider'
    GatewayStore = 'GatewayStore'


class TransportType(object):
    """An Enum class describing the different types of transport components.
    """
    Client = 'client'
    Service = 'service'


def GetConfiguredClass(filePath, className, abstractType):
    fileName = os.path.basename(filePath)
    moduleName, _ = os.path.splitext(fileName)
    configuredClass = getattr(imp.load_source(moduleName, filePath), className)
    if not issubclass(configuredClass, abstractType):
        raise ValueError('Configured class {0}.{1} is not an implementation of {2}'.format(moduleName, className, abstractType))
    return configuredClass

def ProcessConfiguredModule(provider, filePath, className, abstractType, **kwargs):
    configuredClass = GetConfiguredClass(filePath, className, abstractType)
    return configuredClass(infrastructureProvider=provider, **kwargs)

def ProcessConfiguredLogicStore(provider, filePath, className, **kwargs):
    return ProcessConfiguredModule(provider, filePath, className, AbstractLogicStore, **kwargs)

def ProcessConfiguredTransportLayer(provider, filePath, serviceClassName, clientClassName, transportType, **kwargs):
    if transportType == TransportType.Client:
        return ProcessConfiguredModule(provider, filePath, clientClassName, AbstractClientProxy, **kwargs)
    elif transportType == TransportType.Service:
        return ProcessConfiguredModule(provider, filePath, serviceClassName, AbstractServiceImplementations, **kwargs)
    else:
        raise ValueError('Failed to process unknown TransportType {0}'.format(transportType))

def ProcessConfiguredServiceProxyManager(provider, filePath, className, **kwargs):
    return ProcessConfiguredModule(provider, filePath, className, AbstractServiceProxyManager, **kwargs)

def ProcessConfiguredEventPersister(provider, filePath, className, aggregateClass, notifier, **kwargs):
    return ProcessConfiguredModule(provider, filePath, className, AbstractEventPersister, aggregateClass=aggregateClass, notifier=notifier, **kwargs)

def ProcessConfiguredEventReader(provider, filePath, className, aggregateClass, **kwargs):
    return ProcessConfiguredModule(provider, filePath, className, AbstractEventReader, aggregateClass=aggregateClass, **kwargs)

def ProcessConfiguredNotifier(provider, filePath, className, aggregateClass, **kwargs):
    return ProcessConfiguredModule(provider, filePath, className, AbstractNotifier, aggregateClass=aggregateClass, **kwargs)

def ProcessConfiguredCrossAggregateAccess(provider, filePath, className, **kwargs):
    return ProcessConfiguredModule(provider, filePath, className, AbstractCrossAggregateAccess, **kwargs)

def ProcessConfiguredCoreProvider(_, filePath, className, **kwargs): #pylint: disable=W0613
    return GetConfiguredClass(filePath, className, AbstractCoreProvider)

def ProcessConfiguredGatewayStore(provider, filePath, className, **kwargs):
    return ProcessConfiguredModule(provider, filePath, className, AbstractGatewayStore, **kwargs)

class InfrastructureProvider(object):
    """Parses an optional configuration file to determine which "pluggable" components should be used by an instance of the Chronos service.
    The file should have the following format. All sections are optional, but if a section is supplied, it should be fully specified.

    [LogicStore]
    filePath=<path>
    className=<name>

    [TransportLayer]
    filePath=<path>
    serviceClassName=<name>
    clientClassName=<name>

    [ServiceProxyManager]
    filePath=<path>
    className=<name>

    [EventPersister]
    filePath=<path>
    className=<name>

    [EventReader]
    filePath=<path>
    className=<name>

    [Notifier]
    filePath=<path>
    className=<name>

    [CrossAggregateAccess]
    filePath=<path>
    className=<name>

    [CoreProvider]
    filePath=<path>
    className=<name>

    [GatewayStore]
    filePath=<path>
    className=<name>
    """
    ConfigFilePath = '/etc/chronos/config.ini'
    ConfigurablePluginStructure = {ConfigurablePlugin.LogicStore: {'filePath':  os.path.join(os.path.dirname(__file__), 'DefaultImplementations.py'),
                                                                   'className': 'PostgresLogicStore'},
                                   ConfigurablePlugin.TransportLayer: {'filePath': os.path.join(os.path.dirname(__file__), 'RestServiceImplementation.py'),
                                                                       'serviceClassName': 'ChronosGatewayRestService',
                                                                       'clientClassName': 'ChronosGatewayRestClient'},
                                   ConfigurablePlugin.ServiceProxyManager: {'filePath': os.path.join(os.path.dirname(__file__), 'DefaultImplementations.py'),
                                                                            'className': 'ServiceProxyManager'},
                                   ConfigurablePlugin.EventPersister: {'filePath':  os.path.join(os.path.dirname(__file__), 'DefaultImplementations.py'),
                                                                       'className': 'PostgresEventPersister'},
                                   ConfigurablePlugin.EventReader: {'filePath':  os.path.join(os.path.dirname(__file__), 'DefaultImplementations.py'),
                                                                    'className': 'PostgresEventReader'},
                                   ConfigurablePlugin.Notifier: {'filePath':  os.path.join(os.path.dirname(__file__), 'DefaultImplementations.py'),
                                                                 'className': 'RedisNotifier'},
                                   ConfigurablePlugin.CrossAggregateAccess: {'filePath':  os.path.join(os.path.dirname(__file__), 'DefaultImplementations.py'),
                                                                             'className': 'PostgresCrossAggregateAccess'},
                                   ConfigurablePlugin.CoreProvider: {'filePath':  os.path.join(os.path.dirname(__file__), 'DefaultImplementations.py'),
                                                                     'className': 'DefaultCoreProvider'},
                                   ConfigurablePlugin.GatewayStore: {'filePath':  os.path.join(os.path.dirname(__file__), 'DefaultImplementations.py'),
                                                                     'className': 'DefaultGatewayStore'}}
    ConfigurablePluginProcessors = {ConfigurablePlugin.LogicStore: ProcessConfiguredLogicStore,
                                    ConfigurablePlugin.TransportLayer: ProcessConfiguredTransportLayer,
                                    ConfigurablePlugin.ServiceProxyManager: ProcessConfiguredServiceProxyManager,
                                    ConfigurablePlugin.EventPersister: ProcessConfiguredEventPersister,
                                    ConfigurablePlugin.EventReader: ProcessConfiguredEventReader,
                                    ConfigurablePlugin.Notifier: ProcessConfiguredNotifier,
                                    ConfigurablePlugin.CrossAggregateAccess: ProcessConfiguredCrossAggregateAccess,
                                    ConfigurablePlugin.CoreProvider: ProcessConfiguredCoreProvider,
                                    ConfigurablePlugin.GatewayStore: ProcessConfiguredGatewayStore}

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
            if not self.config.read(configFilePath):
                raise EnvironmentError('Unable to read configuration from {0}'.format(configFilePath))

    def OverridesExist(self):
        """Checks whether or not configured overrides were found.

        @returns A boolean representing whether or not a configuration file was found.
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
        """Constructs and returns a specific plugin implementation.

        @param pluginType An instance of a configurable ChronosES plugin. Should be one of the ConfigurablePlugin Enum members.
        """
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
