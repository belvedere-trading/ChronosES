"""@addtogroup Chronos
@{
"""
from abc import ABCMeta, abstractmethod
from threading import Lock
from collections import namedtuple

from redis import StrictRedis

from Chronos.EventLogger import EventLogger
from Chronos.Chronos_pb2 import AggregateLogic

from Chronos.Core import Event
from Chronos.Infrastructure import (AbstractEventStore, AbstractEventKeys, EventPersistenceCheckpoint, FailureSerializationResult, ManagementSerializationResult,
                                    TagSerializationResult, AbstractServiceProxyManager, TransportType, ConfigurablePlugin)

class RedisEventKeys(AbstractEventKeys):
    def __init__(self, aggregateClass):
        super(RedisEventKeys, self).__init__(aggregateClass)
        self.aggregateClass = aggregateClass
        self.hashKey = '{{Chronos.{0}}}'.format(aggregateClass.__name__)
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

    def GetPublishingTopic(self, aggregate):
        formatString = self.aggregateClass.IndexStringFormat.format(**aggregate.GetIndices())
        return self.hashKey + formatString

    def GetFailureTopic(self):
        return 'Failure.{0}'.format(self.hashKey)

    def GetManagementTopic(self):
        return 'Management.{0}'.format(self.hashKey)


class RedisEventStore(AbstractEventStore):
    """An event store implementation using Redis as the backend for both event persistence and
    notification. Each aggregate has its latest state saved to the backing store every time an
    update occurs, and each event persistence is accompanied by a corresponding event
    notification.

    Event persistence atomicity is ensured with MULTI/EXEC transactions, while index atomicity
    is ensured through Lua scripts."""
    ConfigurationSectionName = 'DefaultRedisImplementation'

    def __init__(self, infrastructureProvider, **kwargs): #pylint: disable=W0613
        super(RedisEventStore, self).__init__(infrastructureProvider)
        EventLogger.LogInformationAuto(self, 'Initializing event store')
        self.subscription = None
        self.processingThread = None
        self.lock = Lock()
        try:
            redisConfig = infrastructureProvider.GetSection(self.ConfigurationSectionName)
            host = redisConfig.pop('host', '127.0.0.1')
            port = redisConfig.pop('port', 6379)
            self.redis = StrictRedis(host=host, port=port, **redisConfig)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Failed to initialize event store')
            raise
        EventLogger.LogInformationAuto(self, 'Event store successfully initialized')

    @property
    def KeyGenerator(self):
        return RedisEventKeys

    def Dispose(self):
        with self.lock:
            if self.subscription is not None:
                self.processingThread.stop()
                self.processingThread.join()
                self.subscription.close()
        del self.redis

    def GetAggregateId(self, aggregateClass):
        """Returns the current aggregateId for the supplied aggregate root."""
        keys = RedisEventKeys(aggregateClass)
        aggregateId = self.redis.hgetall(keys.idKey)
        if aggregateId is None:
            return None

        return long(aggregateId)

    def GetAndIncrementAggregateId(self, aggregateClass):
        """Increments the current aggregateId for the supplied aggregate root and returns
        the new value."""
        keys = RedisEventKeys(aggregateClass)
        return long(self.redis.incr(keys.idKey, 1))

    def PersistAggregateLogic(self, aggregateClass, aggregateLogic):
        """Persists the AggregateLogic for the supplied aggregate root and updates the
        logic id for future use."""
        keys = RedisEventKeys(aggregateClass)
        writeLuaScript = WriteAggregateLogic(keys.logicIdKey, keys.logicKey, aggregateLogic.SerializeToString())
        return writeLuaScript.Eval(self.redis)

    def GetLatestAggregateLogic(self, aggregateClass):
        """Returns the latest version of AggregateLogic for the supplied aggregate root."""
        keys = RedisEventKeys(aggregateClass)
        latestLuaScript = GetLatestAggregateLogic(keys.logicIdKey, keys.logicKey)
        return latestLuaScript.Eval(self.redis)

    def GetLatestAggregateLogicByName(self, aggregateName):
        hashKey = '{{Chronos.{0}}}'.format(aggregateName)
        logicIdKey = '{0}.Logic.Id'.format(hashKey)
        logicKey = '{0}.Logic'.format(hashKey)
        logicScript = GetLatestAggregateLogic(logicIdKey, logicKey)
        if logicScript is None:
            raise ValueError('Unable to build IndexStore - no logic found for {0}'.format(hashKey))
        aggregateLogic = logicScript.Eval(self.redis).values()[0]
        return aggregateLogic

    def GetAggregateLogic(self, aggregateClass, logicIds):
        """Retrieves a dictionary of all aggregate logic that matches the provided logic ids."""
        keys = RedisEventKeys(aggregateClass)
        logicResults = []
        for serializedLogic in self.redis.hmget(keys.logicKey, logicIds):
            aggregateLogic = AggregateLogic()
            aggregateLogic.ParseFromString(serializedLogic)
            logicResults.append(aggregateLogic)
        return logicResults

    def PersistEvents(self, aggregateClass, events):
        """Persists all necessary information to the Redis backing store.
        Regardless of Aggregate and Event types, the serialized event will be written
        to the event stream and the serialized aggregate snapshot will replace the
        existing snapshot.

        If the Aggregate has an index key, the index pointer will be rewritten along
        with the other information."""
        # TODO(jkaye): This method is going to need some serious documentation. Events is actually a list of
        # multiprocessing.AsyncResult objects that will eventually contain some SerializationResults.. Going
        # to have to explain that one to the end users
        if len(events) < 1:
            return

        keys = RedisEventKeys(aggregateClass)
        with self.redis.pipeline() as transaction:
            maxRequestId = 0
            aggregateIds = []
            for result in events:
                serializationResult = result.get()
                if isinstance(serializationResult, FailureSerializationResult):
                    transaction.publish(keys.GetFailureTopic(), serializationResult.serializedResponse)
                    continue
                elif isinstance(serializationResult, ManagementSerializationResult):
                    self._publishManagementNotification(transaction, keys, serializationResult.serializedNotification)
                    continue
                elif isinstance(serializationResult, TagSerializationResult):
                    tagKey = keys.GetTagKey(serializationResult.aggregateDict['aggregateId'], serializationResult.tag)
                    transaction.hmset(tagKey, serializationResult.aggregateDict)
                    continue

                aggregateId = serializationResult.aggregateDict['aggregateId']
                eventKey = keys.GetEventKey(aggregateId)
                transaction.rpush(eventKey, serializationResult.serializedEvent)
                snapshotKey = keys.GetSnapshotKey(aggregateId)
                transaction.hmset(snapshotKey, serializationResult.aggregateDict)
                publishingTopic = keys.GetPublishingTopic(serializationResult.aggregate)
                transaction.publish(publishingTopic, serializationResult.serializedResponse)
                aggregateIds.append(aggregateId)
                maxRequestId = serializationResult.requestId
            eventPersistenceCheckpoint = EventPersistenceCheckpoint(maxRequestId, aggregateIds)
            if eventPersistenceCheckpoint.ShouldPersist():
                transaction.hmset(keys.checkpointKey, eventPersistenceCheckpoint.Serialize())
            transaction.execute()

    @staticmethod
    def _publishManagementNotification(redisConnection, keys, serializationResult):
        redisConnection.publish(keys.GetManagementTopic(), serializationResult)

    def PublishManagementNotification(self, aggregateClass, notification):
        keys = RedisEventKeys(aggregateClass)
        self._publishManagementNotification(self.redis, keys, notification.SerializeToString())

    def GetEventPersistenceCheckpoint(self, aggregateClass):
        """Retrieves and deserializes the current EventPersistenceCheckpoint from Redis."""
        keys = RedisEventKeys(aggregateClass)
        serializedCheckpoint = self.redis.hgetall(keys.checkpointKey)
        if not serializedCheckpoint:
            EventLogger.LogInformationAuto(self, 'Unable to find persistence checkpoint', 'Redis result was empty',
                                           tags={'Aggregate': aggregateClass.__name__})
            return None
        checkpoint = EventPersistenceCheckpoint.Deserialize(serializedCheckpoint)
        return checkpoint

    # TODO(jkaye): improve performance on these in the absence of archiving
    def GetEventsToVersion(self, aggregate, toVersion):
        """Retrieves all events for the supplied aggregate's id with version greater than or equal to
        the supplied aggregate's version and less than or equal to the supplied toVersion."""
        keys = RedisEventKeys(aggregate.__class__)
        eventKey = keys.GetEventKey(aggregate.aggregateId)
        events = [Event.FromProtoString(proto) for proto in self.redis.lrange(eventKey, 0, -1)]
        return [singleEvent for singleEvent in events if singleEvent.version >= aggregate.version and singleEvent.version <= toVersion]

    def GetEventsByTimestampRange(self, aggregate, fromTimestamp, toTimestamp):
        """Retrieves all events for the supplied aggregate's id with processedTimestamp greater than or equal to
        the supplied fromTimestamp and less than or equal to the supplied toTimestamp"""
        keys = RedisEventKeys(aggregate.__class__)
        eventKey = keys.GetEventKey(aggregate.aggregateId)
        events = [Event.FromProtoString(proto) for proto in self.redis.lrange(eventKey, 0, -1)]
        return [singleEvent for singleEvent in events if singleEvent.processedTimestamp >= fromTimestamp and singleEvent.processedTimestamp <= toTimestamp]

    def TryGetSnapshot(self, aggregateClass, aggregateId):
        """Attempts to retrieve and return the latest snapshot for the supplied aggregateId.
        If a snapshot cannot be retrieved, None is returned."""
        keys = RedisEventKeys(aggregateClass)
        aggregateDict = self.redis.hgetall(keys.GetSnapshotKey(aggregateId))
        try:
            return aggregateClass.FromDict(aggregateDict)
        except KeyError:
            return None

    def GetAllSnapshots(self, aggregateClass):
        """Retrieves all snapshots for the supplied aggregate root."""
        aggregates = []
        currentAggregateId = self.GetAggregateId(aggregateClass)
        if currentAggregateId:
            for aggregateId in range(1, currentAggregateId + 1):
                aggregate = self.TryGetSnapshot(aggregateClass, aggregateId)
                if aggregate is not None:
                    aggregates.append(aggregate)

        return aggregates

    def GetIndexedSnapshots(self, aggregateClass, aggregateIds):
        """Retrieves all aggregates for the supplied aggregate root that satisfy the provided index contraints."""
        keys = RedisEventKeys(aggregateClass)
        indexKeys = [keys.GetSnapshotKey(aggregateId) for aggregateId in aggregateIds]
        luaScript = GetSnapshotsFromIndex(indexKeys)
        return [aggregateClass.FromDict(dct) for dct in luaScript.Eval(self.redis)]

    def GetTags(self, aggregateClass):
        keys = RedisEventKeys(aggregateClass)
        tagKeys = self.redis.scan_iter(keys.GetTagWildcard())
        with self.redis.pipeline as transaction:
            for key in tagKeys:
                transaction.hgetall(key)
            snapshots = transaction.execute()
        tags = [tagKey.split('.', 5)[5] for tagKey in tagKeys]
        return dict(zip(tags, snapshots))

    def GetTag(self, aggregateClass, aggregateId, tag):
        keys = RedisEventKeys(aggregateClass)
        tagKey = keys.GetTagKey(aggregateId, tag)
        tagDict = self.redis.hgetall(tagKey)
        if not tagDict:
            return None
        return aggregateClass.FromDict(tagDict)

    def GetAllAggregateNames(self):
        return [aggregate.split('.')[1] for aggregate in self.redis.scan_iter('{{Chronos.*}}.Logic')]

    def Exists(self, aggregateClass, key): #pylint: disable=W0613
        return self.redis.exists(key)

    def NumEvents(self, aggregateClass, aggregateId):
        keys = RedisEventKeys(aggregateClass)
        return self.redis.llen(keys.GetEventKey(aggregateId))

    def GetAllLogic(self, aggregateClass):
        keys = RedisEventKeys(aggregateClass)
        return self.redis.hgetall(keys.logicKey)

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

    def Subscribe(self, prefix, channel, callback):
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

    def Unsubscribe(self, prefix, channel):
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

ServiceEndpoint = namedtuple('ServiceEndpoint', ['host', 'port'])

def GetConfiguredBinding(infrastructureProvider):
    config = infrastructureProvider.GetSection('DefaultTransportLayerImplementation')
    host = config.pop('host', '0.0.0.0')
    port = int(config.pop('port', 18256))
    return host, port

class ServiceProxyManager(AbstractServiceProxyManager):
    def __init__(self, infrastructureProvider, **kwargs): #pylint: disable=W0613
        AbstractServiceProxyManager.__init__(self, infrastructureProvider)
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


class LuaScript(object):
    __metaclass__ = ABCMeta
    """A simple base class to abstract the redis internals of
    Lua scripts away from the client."""
    def __init__(self, luaScript, keys=None, argv=None):
        self.luaScript = luaScript
        self.keys = keys if keys is not None else []
        self.argv = argv if argv is not None else []

    @abstractmethod
    def ParseRedisResult(self, redisResult):
        pass

    def Eval(self, redisConnection):
        script = redisConnection.register_script(self.luaScript)
        result = script(self.keys, self.argv)
        return self.ParseRedisResult(result)


class GetSnapshotsFromIndex(LuaScript):
    """A Lua script for transactional retrieval of snapshots from index pointers."""
    IndexLuaScript = """local snapshots = {}
                        for _, key in ipairs(KEYS) do
                            table.insert(snapshots, redis.call('hgetall', key))
                        end
                        return snapshots"""
    def __init__(self, keys):
        super(GetSnapshotsFromIndex, self).__init__(self.IndexLuaScript, keys=keys)

    def ParseRedisResult(self, redisResult):
        return [dict(result[i:i + 2] for i in range(0, len(result), 2)) for result in redisResult]

class WriteAggregateLogic(LuaScript):
    """A Lua script for atmoic updating of AggregateLogic information"""
    LogicLuaScript = """local logicId = redis.call('incr', KEYS[1])
                        redis.call('hset', KEYS[2], logicId, ARGV[1])
                        return logicId"""
    def __init__(self, logicIdKey, logicKey, aggregateLogic):
        super(WriteAggregateLogic, self).__init__(self.LogicLuaScript,
                                                  keys=(logicIdKey, logicKey),
                                                  argv=(aggregateLogic,))

    def ParseRedisResult(self, redisResult):
        return redisResult

class GetLatestAggregateLogic(LuaScript):
    """A Lua script for retrieval of the latest AggregateLogic for a given Aggregate"""
    LogicLuaScript = """local logicId = redis.call('get', KEYS[1])
                        local logicMap = {}
                        if logicId then
                          table.insert(logicMap, logicId)
                          table.insert(logicMap, redis.call('hget', KEYS[2], logicId))
                        end
                        return logicMap"""
    def __init__(self, logicIdKey, logicKey):
        super(GetLatestAggregateLogic, self).__init__(self.LogicLuaScript,
                                                      keys=(logicIdKey, logicKey))

    def ParseRedisResult(self, redisResult):
        if not redisResult:
            EventLogger.LogInformationAuto(self, 'Unable to get aggregate logic', 'Script result was None')
            return None

        logicId, serializedLogic = redisResult
        aggregateLogic = AggregateLogic()
        aggregateLogic.ParseFromString(serializedLogic)
        return {long(logicId): aggregateLogic}
