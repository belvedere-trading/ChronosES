"""@addtogroup Chronos
@{
"""
import functools

from threading import Lock
from collections import namedtuple

from redis import StrictRedis, exceptions as RedisExceptions

from Chronos.EventLogger import EventLogger
from Chronos.Chronos_pb2 import AggregateLogic

from Chronos.Core import (FailureSerializationResult, ManagementSerializationResult, SuccessSerializationResult, TagSerializationResult, Event, ConstraintComparer,
                          ConstraintSerializer, ChronosCoreException, ChronosFailedEventPersistenceException)
from Chronos.PostgresBackend import (PostgresLogicConnection, PostgresEventReadConnection, PostgresEventWriteConnection, PostgresCrossAggregateConnection,
                                     DEFAULTINSTANCESTEP, DEFAULTEVENTSTEP)
from Chronos.Infrastructure import (AbstractCoreProvider, AbstractEventPersistenceCheckpoint, AbstractNotifier, AbstractEventPersister, AbstractEventReader,
                                    AbstractCrossAggregateAccess, AbstractLogicStore, AbstractServiceProxyManager, TransportType, ConfigurablePlugin,
                                    AbstractGatewayStore)

class ChronosTransactionException(Exception):
    """An Exception indicating that the notifier attempted to commit a transaction without a transaction previously being opened. `BeginTransaction()` must be called
    prior to committing a transaction."""
    pass


class ChronosTransactionInProgressException(Exception):
    """An Exception indicating that the notifier attempted to open a transaction while another transaction was already opened. Only one transaction is allowed to be
    opened at a time."""
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


class DefaultGatewayStore(AbstractGatewayStore):
    def __init__(self, infrastructureProvider, **kwargs): #pylint: disable=W0613
        super(DefaultGatewayStore, self).__init__(infrastructureProvider)
        postgresUser, postgresPassword, postgresDatabase, postgresHost, postgresPort = GetPostgresConnectionDetails(self.infrastructureProvider)

        self.logicConnection = PostgresLogicConnection(postgresUser, postgresPassword, postgresDatabase, postgresHost, postgresPort)
        self._logicStore = self.infrastructureProvider.GetConfigurablePlugin(ConfigurablePlugin.LogicStore, postgresConnection=self.logicConnection) #pylint: disable=C0103

        self.crossAggregateConnection = PostgresCrossAggregateConnection(postgresUser, postgresPassword, postgresDatabase, postgresHost, postgresPort)
        self._crossAggregateAccess = self.infrastructureProvider.GetConfigurablePlugin(ConfigurablePlugin.CrossAggregateAccess, #pylint: disable=C0103
                                                                                       postgresConnection=self.crossAggregateConnection)

    @property
    def logicStore(self):
        return self._logicStore

    @property
    def crossAggregateAccess(self):
        return self._crossAggregateAccess

    def Dispose(self):
        self.logicConnection.Dispose()
        self.crossAggregateConnection.Dispose()


class EventPersistenceCheckpoint(AbstractEventPersistenceCheckpoint):
    def __init__(self, maxRequestId, **kwargs): #pylint: disable=W0613
        super(EventPersistenceCheckpoint, self).__init__(maxRequestId)

    def ShouldPersist(self):
        return self.maxRequestId > 0

    def Serialize(self):
        return {'maxRequestId': self.maxRequestId}

    @staticmethod
    def Deserialize(dct):
        return EventPersistenceCheckpoint(long(dct['maxRequestId']))


class RedisEventKeys(object):
    def __init__(self, aggregateClass):
        self.aggregateClass = aggregateClass
        self.hashKey = 'Chronos.{0}'.format(aggregateClass.__name__)
        self.idKey = '{0}.Id'.format(self.hashKey)
        self.logicIdKey = '{0}.Logic.Id'.format(self.hashKey)
        self.logicKey = '{0}.Logic'.format(self.hashKey)
        self.checkpointKey = '{0}.Checkpoint'.format(self.hashKey)

    def GetEventKey(self, aggregateId):
        return '{0}.Event.{1}'.format(self.hashKey, aggregateId)

    def GetSnapshotKey(self, aggregateId):
        return '{0}.Snapshot.{1}'.format(self.hashKey, aggregateId)

    def GetTagWildcard(self):
        return 'Tag.{0}.Tag.*'.format(self.hashKey)

    def GetTagKey(self, aggregateId, tag):
        return 'Tag.{0}.Tag.{1}.{2}'.format(self.hashKey, aggregateId, tag)

    def GetPublishingTopic(self, aggregateIndices):
        formatString = self.aggregateClass.IndexStringFormat.format(**aggregateIndices)
        return self.hashKey + formatString

    def GetFailureTopic(self):
        return 'Failure.{0}'.format(self.hashKey)

    def GetManagementTopic(self):
        return 'Management.{0}'.format(self.hashKey)


def redis_operation(function): #pylint: disable=C0103
    @functools.wraps(function)
    def Wrapper(self, *args):
        if self.isRunning:
            try:
                return function(self, *args)
            except RedisExceptions.ConnectionError:
                EventLogger.LogExceptionAuto(self, 'Attempting to reconnect to Redis', 'Connection to Redis lost')
                raise ChronosFailedEventPersistenceException('Lost connection to Redis.')
    return Wrapper

class RedisNotifier(AbstractNotifier):
    def __init__(self, infrastructureProvider, aggregateClass, redisConnection, **kwargs): #pylint: disable=W0613
        super(RedisNotifier, self).__init__(infrastructureProvider, aggregateClass)
        self.subscription = None
        self.processingThread = None
        self.lock = Lock()
        self.redis = redisConnection
        self.keys = RedisEventKeys(self.aggregateClass)
        self.isRunning = True
        self.transaction = None

    def Rollback(self):
        self.transaction = None

    def Dispose(self):
        with self.lock:
            self.isRunning = False
            if self.subscription is not None:
                self.processingThread.stop()
                self.processingThread.join()
                self.subscription.close()

    @redis_operation
    def PublishGatewayManagementNotification(self, notification):
        self.redis.publish(self.keys.GetManagementTopic(), notification.SerializeToString())

    @redis_operation
    def PublishManagementNotification(self, notification):
        if self.transaction is None:
            raise ChronosTransactionException('No transaction open to publish to.')
        self.transaction.publish(self.keys.GetManagementTopic(), notification)

    @redis_operation
    def PublishFailureNotification(self, notification):
        if self.transaction is None:
            raise ChronosTransactionException('No transaction open to publish to.')
        self.transaction.publish(self.keys.GetFailureTopic(), notification)

    @redis_operation
    def PublishPersistedEventNotification(self, aggregateIndices, notification):
        if self.transaction is None:
            raise ChronosTransactionException('No transaction open to publish to.')
        self.transaction.publish(self.keys.GetPublishingTopic(aggregateIndices), notification)

    @redis_operation
    def GetEventPersistenceCheckpoint(self):
        """Retrieves and deserializes the current EventPersistenceCheckpoint from Redis."""
        serializedCheckpoint = self.redis.hgetall(self.keys.checkpointKey)
        if not serializedCheckpoint:
            EventLogger.LogInformationAuto(self, 'Unable to find persistence checkpoint', 'Redis result was empty',
                                           tags={'Aggregate': self.aggregateClass.__name__})
            return None
        checkpoint = EventPersistenceCheckpoint.Deserialize(serializedCheckpoint)
        return checkpoint

    @redis_operation
    def BeginTransaction(self):
        if self.transaction is not None:
            raise ChronosTransactionInProgressException('Only one transaction can be opened at once')
        self.transaction = self.redis.pipeline()

    @redis_operation
    def CommitTransaction(self, eventPersistenceCheckpoint):
        if self.transaction is None:
            raise ChronosTransactionException('No transaction open to commit')

        self.transaction.hmset(self.keys.checkpointKey, eventPersistenceCheckpoint.Serialize())
        self.transaction.execute()
        self.transaction = None

    def GetSubscriptionChannel(self, indices, indexedAttributes):
        subscriptionChannel = self.keys.hashKey
        if len(indexedAttributes) > 0:
            subscriptionChannel += '.' + '.'.join([self._getIndexedAttribute(attr, indices) for attr in indexedAttributes])
        return subscriptionChannel

    @staticmethod
    def _getIndexedAttribute(indexedAttribute, indices):
        attr = str(indices.get(indexedAttribute.attributeName, '*'))
        if indexedAttribute.isCaseInsensitive:
            attr = attr.lower()
        return attr

    def _getSubscription(self):
        with self.lock:
            if self.subscription is None:
                try:
                    self.subscription = self.redis.pubsub(ignore_subscribe_messages=True)
                except Exception:
                    EventLogger.LogExceptionAuto(self, 'Error creating subscription connection')
                    raise
        return self.subscription

    def _startProcessingIfNecessary(self):
        with self.lock:
            if self.processingThread is None:
                self.processingThread = self.subscription.run_in_thread(sleep_time=.001)

    @staticmethod
    def _isPattern(channel):
        return '*' in channel

    def SubscribeForManagement(self, callback):
        self.Subscribe(self.keys.GetManagementTopic(), callback)

    def SubscribeForFailure(self, callback):
        self.Subscribe(self.keys.GetFailureTopic(), callback)

    def Subscribe(self, channel, callback):
        subscription = self._getSubscription()
        try:
            if self._isPattern(channel):
                subscription.psubscribe(**{channel: callback})
            else:
                subscription.subscribe(**{channel: callback})
            self._startProcessingIfNecessary()
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error subscribing', tags={'Channel': channel})
            raise

    def UnsubscribeForManagement(self):
        self.Unsubscribe(self.keys.GetManagementTopic())

    def UnsubscribeForFailure(self):
        self.Unsubscribe(self.keys.GetFailureTopic())

    def Unsubscribe(self, channel):
        subscription = self._getSubscription()
        try:
            if self._isPattern(channel):
                subscription.punsubscribe(channel)
            else:
                subscription.unsubscribe(channel)
            self._startProcessingIfNecessary()
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error unsubscribing', tags={'Channel': channel})
            raise


StagedEvent = namedtuple('StagedEvent', ['requestId', 'indexedAttributes', 'serializedResponse'])

class PostgresEventPersister(AbstractEventPersister):
    def __init__(self, infrastructureProvider, aggregateClass, notifier, postgresConnection, **kwargs): #pylint: disable=W0613
        super(PostgresEventPersister, self).__init__(infrastructureProvider, aggregateClass, notifier)
        self.storageConnection = postgresConnection
        self.eventTypeCache = {}
        self.eventPartitionBoundary = None
        self.instancePartitionBoundary = None

    def Begin(self):
        self.storageConnection.CreateSavepoint(self.transactionSavepointName)

    def Commit(self):
        self.storageConnection.DeleteSavepoint(self.transactionSavepointName)

    def Rollback(self):
        self.storageConnection.RollbackToSavepoint(self.transactionSavepointName)
        self.storageConnection.DeleteSavepoint(self.transactionSavepointName)

    def RollbackNotifier(self):
        self.notifier.Rollback()

    def PersistEvents(self, events):
        maxRequestId = 0
        self.notifier.BeginTransaction()

        for event in events:
            if isinstance(event, FailureSerializationResult):
                self.notifier.PublishFailureNotification(event.serializedResponse)

            elif isinstance(event, ManagementSerializationResult):
                self.notifier.PublishManagementNotification(event.serializedNotification)

            elif isinstance(event, TagSerializationResult):
                self.storageConnection.InsertTag(event.aggregateDict['aggregateId'], event.aggregateDict['logicId'], event.tag, event.aggregateDict['proto'],
                                                 event.aggregateDict['version'], event.aggregateDict['createDate'])

            elif isinstance(event, SuccessSerializationResult):
                eventTypeId = self._getEventTypeId(event.eventProto.type)
                currentEventId = self.storageConnection.InsertEvent(event.aggregateDict['aggregateId'], event.aggregateDict['logicId'], eventTypeId,
                                                                    event.eventProto.proto, event.eventProto.version, event.eventProto.receivedTimestamp,
                                                                    event.eventProto.processedTimestamp)
                self.storageConnection.UpdateEventPersistenceCheckpoint(event.requestId, event.aggregate.GetIndices(), event.serializedResponse)
                self._checkAndUpdatePartitions(currentEventId, event.aggregateDict['aggregateId'])
                maxRequestId = event.requestId
                self.notifier.PublishPersistedEventNotification(event.aggregate.GetIndices(), event.serializedResponse)
            else:
                EventLogger.LogErrorAuto(self, 'Unknown serialization result type received while persisting events',
                                         tags={'Aggregate': self.aggregateName, 'Type': type(event).__name__})

        eventPersistenceCheckpoint = EventPersistenceCheckpoint(maxRequestId)
        if eventPersistenceCheckpoint.ShouldPersist():
            self.storageConnection.Commit()
        self.notifier.CommitTransaction(eventPersistenceCheckpoint)
        self.storageConnection.FlushStagedEvents()

    def _getEventTypeId(self, eventTypeString):
        if eventTypeString not in self.eventTypeCache:
            self.eventTypeCache[eventTypeString] = self.storageConnection.InsertOrGetEventTypeId(eventTypeString)
        return self.eventTypeCache[eventTypeString]

    def _checkAndUpdatePartitions(self, eventId, instanceId):
        if not self.eventPartitionBoundary or not self.instancePartitionBoundary:
            self.eventPartitionBoundary, self.instancePartitionBoundary = self.storageConnection.GetCurrentPartitionBoundaries()
        if eventId >= self.eventPartitionBoundary:
            self._createNewEventPartitionTable()
        if instanceId >= self.instancePartitionBoundary:
            self._createNewInstancePartitionTable()

    def _createNewEventPartitionTable(self):
        self.eventPartitionBoundary = self.storageConnection.CreateNewEventPartitionTable(self.eventPartitionBoundary, DEFAULTEVENTSTEP)

    def _createNewInstancePartitionTable(self):
        self.instancePartitionBoundary = self.storageConnection.CreateNewInstancePartitionTable(self.instancePartitionBoundary, DEFAULTINSTANCESTEP)

    def TryVerifyEventPersistenceCheckpoint(self):
        try:
            stagedEvents = self._getStagedEvents()
            if len(stagedEvents) == 0 or stagedEvents is None:
                return
            EventLogger.LogWarningAuto(self, 'Persisted events staged for notification found.')
            eventPersistenceCheckpoint = self.notifier.GetEventPersistenceCheckpoint()
            if eventPersistenceCheckpoint is not None:
                if stagedEvents[0].requestId == eventPersistenceCheckpoint.maxRequestId:
                    EventLogger.LogWarningAuto(self, 'Persisted events staged for notification match most recently notified. No need to notify')
                    return
            EventLogger.LogWarningAuto(self, 'Notifying for staged events', 'Persisted events and notfications have diverged.',
                                       tags={'Aggregate': self.aggregateName})

            self._publishStagedNotifications(stagedEvents)

        except Exception:
            EventLogger.LogExceptionAuto(self, 'Checkpoint verification failed', tags={'Aggregate': self.aggregateName})
            raise ChronosFailedEventPersistenceException('Failed to publish notifications while rebuilding from persistence checkpoint')

    def _getStagedEvents(self):
        results = self.storageConnection.GetStagedEvents()
        if results is None:
            return None
        return [StagedEvent(*result) for result in results]

    def _publishStagedNotifications(self, stagedEvents):
        maxRequestId = 0
        self.notifier.BeginTransaction()
        for stagedEvent in stagedEvents:
            try:
                self.notifier.PublishPersistedEventNotification(stagedEvent.indexedAttributes, stagedEvent.serializedResponse)
                maxRequestId = stagedEvent.requestId
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Publishing notifications failed while rebuilding from persistence checkpoint',
                                             tags={'Aggregate': self.aggregateName,
                                                   'StagedEvent': stagedEvent})
                self.RollbackNotifier()
                raise ChronosFailedEventPersistenceException('Failed to publish notifications while rebuilding from persistence checkpoint')
        try:
            eventPersistenceCheckpoint = EventPersistenceCheckpoint(maxRequestId)
            if eventPersistenceCheckpoint.ShouldPersist():
                self.notifier.CommitTransaction(eventPersistenceCheckpoint)
                self.storageConnection.FlushStagedEvents()

        except Exception:
            EventLogger.LogExceptionAuto(self, 'Failed to update index checkpoint',
                                         tags={'Aggregate': self.aggregateName,
                                               'MaxRequestId': maxRequestId})

    def UpsertAggregateSnapshot(self, aggregate):
        self.storageConnection.UpsertAggregateInstance(aggregate.aggregateId, aggregate.proto.SerializeToString(), aggregate.GetIndices())


class PostgresEventReader(AbstractEventReader):
    def __init__(self, infrastructureProvider, aggregateClass, postgresConnection, **kwargs): #pylint: disable=W0613
        super(PostgresEventReader, self).__init__(infrastructureProvider, aggregateClass)
        self.aggregateClass = aggregateClass
        self.aggregateName = aggregateClass.__name__
        self.storageConnection = postgresConnection
        self.currentAggregateId = self.storageConnection.GetCurrentAggregateId()

    def GetNextAggregateId(self):
        self.currentAggregateId += 1
        return self.currentAggregateId

    def GetEventsToVersion(self, aggregate, toVersion):
        resultList = self.storageConnection.GetEventsToVersion(aggregate.aggregateId, aggregate.version, toVersion)
        return [Event.CreateInstance(*row) for row in resultList]

    def GetEventsByTimestampRange(self, aggregate, fromTimestamp, toTimestamp):
        resultList = self.storageConnection.GetEventsByTimestampRange(aggregate.aggregateId, fromTimestamp, toTimestamp)
        return [Event.CreateInstance(*row) for row in resultList]

    def TryGetSnapshot(self, aggregateId):
        returnedTuple = self.storageConnection.GetSnapshot(aggregateId)
        if returnedTuple is None:
            return None
        return self._snapshotFromTuple(*returnedTuple)

    def GetAllSnapshots(self):
        returnedList = self.storageConnection.GetAllSnapshots()
        return [self._snapshotFromTuple(*returnedTuple) for returnedTuple in returnedList]

    def GetIndexedSnapshots(self, indexDict):
        returnedList = self.storageConnection.GetIndexedSnapshots(self.aggregateClass.NoCaseAttributes, indexDict)
        return [self._snapshotFromTuple(*returnedTuple) for returnedTuple in returnedList]

    def TryGetTag(self, aggregateId, tag):
        returnedTuple = self.storageConnection.GetTag(aggregateId, tag)
        if returnedTuple is None:
            return None
        return self._snapshotFromTuple(*returnedTuple)

    def GetTags(self, aggregateId):
        result = self.storageConnection.GetTags(aggregateId)
        return [row[0] for row in result]

    def RetrieveAggregateIds(self, **indices):
        if not self.aggregateClass.IndexedAttributes:
            raise ChronosCoreException('Aggregate {0} is not indexed'.format(self.aggregateClass.__name__))
        result = self.storageConnection.GetIndexedSnapshots(self.aggregateClass.NoCaseAttributes, indices)
        return [row[0] for row in result]

    def RetrieveAggregates(self, startRow, returnCount, sortField, sortDescending, **indices):
        if not self.aggregateClass.IndexedAttributes:
            raise ChronosCoreException('Aggregate {0} is not indexed'.format(self.aggregateClass.__name__))
        count, returnedList = self.storageConnection.RetrieveAggregates(startRow, returnCount, sortField, sortDescending, self.aggregateClass.NoCaseAttributes, indices)
        return count, [self._retrieveAggregatesWithAggregateId(*returnedTuple) for returnedTuple in returnedList]

    def _snapshotFromTuple(self, aggregateId, snapshot, version):
        # NOTE: increment the version because Chronos expects the aggregate version to be 1 higher than the last applied event
        return self.aggregateClass.FromDict({'aggregateId': aggregateId, 'proto': snapshot, 'version': version + 1})

    def _retrieveAggregatesWithAggregateId(self, aggregateId, snapshot, version):
        # NOTE: increment the version because Chronos expects the aggregate version to be 1 higher than the last applied event
        indices = self._snapshotFromTuple(aggregateId, snapshot, version).GetIndices()
        aggregateIdDict = {'aggregateId':aggregateId}
        aggregateIdDict.update(indices)
        return aggregateIdDict

AggregateStorageMetadata = namedtuple('AggregateStorageMetadata', ['aggregateId', 'instancePartitionBoundary', 'eventPartitionBoundary'])

class PostgresCrossAggregateAccess(AbstractCrossAggregateAccess):
    def __init__(self, infrastructureProvider, postgresConnection, **kwargs): #pylint: disable=W0613
        super(PostgresCrossAggregateAccess, self).__init__(infrastructureProvider)
        self.storageConnection = postgresConnection

    def GetOrCreateMetadata(self, aggregateName):
        try:
            result = self.storageConnection.GetAggregateMetadata(aggregateName)
            if result is None:
                result = self.storageConnection.CreateInitialSchema(aggregateName, DEFAULTINSTANCESTEP, DEFAULTEVENTSTEP)
            self.storageConnection.Commit()
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Exception thrown during underlying storage access',
                                         tags={'Aggregate': aggregateName})
            self.storageConnection.Rollback()
            raise
        return result[0]

    def GetAllAggregateNames(self):
        return [row[0] for row in self.storageConnection.GetAllAggregateNames()]


ChronosLogic = namedtuple('ChronosLogic', ['logicId', 'logicVersion', 'pythonLogic', 'protoContract', 'constraints'])
LogicMetadata = namedtuple('LogicMetadata', ['logicId', 'logicVersion'])

class PostgresLogicStore(AbstractLogicStore):
    def __init__(self, infrastructureProvider, postgresConnection, **kwargs): #pylint: disable=W0613
        super(PostgresLogicStore, self).__init__(infrastructureProvider)
        self.storageConnection = postgresConnection
        self.serializer = ConstraintSerializer()

    def PersistAggregateLogic(self, aggregateClassId, aggregateName, logicVersion, aggregateLogic, module):
        try:
            oldConstraints = None
            newConstraints = module.AggregateClass.Constraints
            serializedNewConstraints = self.serializer.Serialize(newConstraints)
            if logicVersion != 1:
                oldConstraints = self.storageConnection.GetLatestConstraints(aggregateName)

            comparer = ConstraintComparer(newConstraints, self.serializer.Deserialize(oldConstraints), serializedNewConstraints, oldConstraints)
            logicId = self.storageConnection.InsertAggregateLogic(aggregateClassId, logicVersion, aggregateLogic.pythonFileContents, aggregateLogic.protoFileContents,
                                                                  serializedNewConstraints)
            divergence = comparer.UpdateConstraints()
            if len(divergence.constraintsToAdd) == 0 and len(divergence.constraintsToRemove) == 0:
                EventLogger.LogInformationAuto(self, 'Skipping constraint update', 'Constraints have not diverged', tags={'Aggregate': aggregateName})

            else:
                EventLogger.LogInformationAuto(self, 'Removing constraints', tags={'Constraints':divergence.constraintsToRemove})
                for constraint in divergence.constraintsToRemove:
                    constraint.Drop(aggregateName, self.storageConnection)
                EventLogger.LogInformationAuto(self, 'Adding constraints', tags={'Constraints':divergence.constraintsToAdd})
                for constraint in divergence.constraintsToAdd:
                    constraint.Create(aggregateName, self.storageConnection)

            self.storageConnection.Commit()
            return LogicMetadata(logicId, logicVersion)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Exception occurred while persisting logic', tags={'Aggregate': aggregateName})
            self.storageConnection.Rollback()
            raise

    def GetLatestAggregateLogic(self, aggregateName):
        result = self.storageConnection.GetLatestAggregateLogic(aggregateName)
        if result is None:
            return None
        logic = ChronosLogic(*result)
        return LogicMetadata(logic.logicId, logic.logicVersion), AggregateLogic(protoFileContents=logic.protoContract,
                                                                                pythonFileContents=logic.pythonLogic)

    def GetPreviousAggregateLogics(self, aggregateName, logicVersions):
        resultList = self.storageConnection.GetPreviousAggregateLogics(aggregateName, logicVersions)
        returnDict = {}
        for row in resultList:
            logic = ChronosLogic(*row)
            returnDict[logic.logicVersion] = AggregateLogic(protoFileContents=logic.protoContract, pythonFileContents=logic.pythonLogic)
        return returnDict


ServiceEndpoint = namedtuple('ServiceEndpoint', ['host', 'port'])

def GetConfiguredBinding(infrastructureProvider):
    config = infrastructureProvider.GetSection('DefaultTransportLayerImplementation')
    host = config.pop('host', '0.0.0.0')
    port = int(config.pop('port', 18256))
    return host, port

class ServiceProxyManager(AbstractServiceProxyManager):
    def __init__(self, infrastructureProvider, clientProxyChangedFunc, **kwargs): #pylint: disable=W0613
        AbstractServiceProxyManager.__init__(self, infrastructureProvider, clientProxyChangedFunc)
        try:
            host, port = GetConfiguredBinding(infrastructureProvider)
            self.serviceEndpoint = ServiceEndpoint(host, port)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Failed to initialize service proxy manager')
            raise
        EventLogger.LogInformationAuto(self, 'Service proxy manager successfully initialized')
        self.client = None

    def Dispose(self):
        self.Disconnect()
        self.client = None

    def Connect(self):
        self.client = self.infrastructureProvider.GetConfigurablePlugin(ConfigurablePlugin.TransportLayer, transportType=TransportType.Client)
        self.client.Startup(self.serviceEndpoint)
        return self.client

    def Disconnect(self):
        self.client.Shutdown()
