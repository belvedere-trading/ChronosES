#pylint: disable=C0302
"""@addtogroup Chronos
@{
"""
import copy
import glob
import imp
import os
import subprocess
import sys
import threading
import time
from multiprocessing import Lock, Pool

from google.protobuf import message
from sqlalchemy import Column, BigInteger, Boolean, UniqueConstraint, Integer, String, create_engine, event
from sqlalchemy.ext.declarative import as_declarative, declared_attr, DeclarativeMeta, AbstractConcreteBase
from sqlalchemy.orm import sessionmaker

from Chronos.EventLogger import EventLogger
from Chronos.Map import LRUEvictingMap
from Chronos.Chronos_pb2 import EventProto, ChronosManagementNotification

from Chronos.Infrastructure import (PersistenceBufferItem, PersistenceBufferFailureItem, PersistenceBufferManagementItem, PersistenceBufferTagItem,
                                    InfrastructureProvider, ConfigurablePlugin)

class ChronosCoreException(Exception):
    """An Exception indicating that Chronos has encountered an error from
    which it is unable to recover. This Exception should only be thrown if
    the API is being used in an incorrect manner."""
    pass


class ChronosSemanticException(Exception):
    """An Exception indicating that Chronos has encountered a class that
    violates required semantics for event sourcing or notification."""
    pass

class ValidationError(Exception):
    """An Exception for use in Event subclasses to indicate that the event is logically invalid.
    Raising this Exception from within Event::RaiseFor will still cause the event application to fail,
    but the failure will not be logged as an error.
    """
    def __init__(self, exceptionMessage, tags=None):
        super(ValidationError, self).__init__(exceptionMessage)
        self.tags = tags

class ChronosDeclarativeMeta(DeclarativeMeta):
    """A metaclass that tracks the current state of a Chronos declarative index.
    This is required because SQLAlchemy doesn't have a built in way to remove columns
    that have already been attached to a table. Instead, we must manually ignore the 'extra'
    columns through __mapper_args__."""
    def __init__(cls, classname, bases, dct):
        cls.IndexedAttributes = set() #pylint: disable=C0103
        for attr, value in dct.iteritems():
            if isinstance(value, Column):
                cls.IndexedAttributes.add(attr)
        super(ChronosDeclarativeMeta, cls).__init__(classname, bases, dct)

@as_declarative(metaclass=ChronosDeclarativeMeta)
class ChronosDeclarativeBase(object):
    BaseConfig = {'extend_existing': True}
    constraints = []

    @declared_attr
    def __table_args__(cls): #pylint: disable=E0213
        if not cls.constraints:
            return cls.BaseConfig
        if cls.BaseConfig not in cls.constraints:
            cls.constraints.append(cls.BaseConfig)
        return tuple(cls.constraints)

    def ToJson(self):
        return dict((c.name, getattr(self, c.name)) for c in self.__table__.columns) #pylint: disable=E1101

class ChronosIndex(AbstractConcreteBase, ChronosDeclarativeBase):
    """The declarative base class for Chronos indices.
    This class expects subclasses to declare any aggregate properties that should be indexed
    as SQLAlchemy Columns. Any constraints or sql indices should be provided via the
    constraints classmember.

    It is important that the class name of a Chronos index class never changes - this would
    cause a discrepancy in index table names through time and could result in permanent data loss."""
    SharedProperties = ('aggregateId', 'isArchived', 'maxArchivedVersion')

    @declared_attr
    def __tablename__(cls): #pylint: disable=E0213
        return cls.__name__.lower() #pylint: disable=E1101

    @declared_attr
    def __mapper_args__(cls): #pylint: disable=E0213
        if cls == ChronosIndex:
            return {}
        properties = list(cls.IndexedAttributes) #pylint: disable=E1101
        properties.extend(cls.SharedProperties)
        return {'include_properties': properties, 'concrete': True,
                'polymorphic_identity': cls.__tablename__}

    aggregateId = Column(BigInteger, primary_key=True, autoincrement=False, nullable=False)
    isArchived = Column(Boolean, nullable=False, default=False)
    maxArchivedVersion = Column(BigInteger, nullable=False, default=0)

    @classmethod
    def CreateTagModel(cls):
        class ChronosTag(ChronosDeclarativeBase):
            """The SQLAlchemy model for Chronos tag databases.
            Each Aggregate has its own tag database with the same schema (as specified by this class).
            This database is used to ensure consistency of tags across both live and archived data,
            as well as improving the performance of the history service by providing metadata about tags
            that would not be easily accessible otherwise."""
            constraints = [UniqueConstraint('aggregateId', 'tag', name='uix_aggregateId_tag')]

            @declared_attr
            def __tablename__(kls): #pylint:disable=E0213,R0201
                return cls.__name__.lower() + '_tag'

            @declared_attr
            def __mapper_args__(kls): #pylint:disable=E0213
                return {'include_properties': ['tagId', 'aggregateId', 'tag', 'createDate', 'isArchived'], 'concrete': True,
                        'polymorphic_identity': kls.__tablename__}



            tagId = Column(Integer, primary_key=True, nullable=False)
            aggregateId = Column(BigInteger, nullable=False)
            tag = Column(String, nullable=False)
            createDate = Column(BigInteger, nullable=False)
            isArchived = Column(Boolean, default=False, nullable=True)
        return ChronosTag

    @classmethod
    def CreateCheckpointModel(cls):
        class ChronosCheckpoint(ChronosDeclarativeBase):
            """The SQLAlchemy model for Chronos checkpoint databases."""
            @declared_attr
            def __tablename__(kls): #pylint:disable=E0213,R0201
                return cls.__name__.lower() + '_checkpoint'

            @declared_attr
            def __mapper_args__(kls): #pylint:disable=E0213
                return {'include_properties': ['checkpointId', 'maxRequestId'], 'concrete': True,
                        'polymorphic_identity': kls.__tablename__}

            checkpointId = Column(Integer, primary_key=True, nullable=False)
            maxRequestId = Column(Integer, unique=True, nullable=False)
        return ChronosCheckpoint

class ChronosBase(object):
    """Provides common functionality for Chronos base classes, Aggregate and Event.
    Any subclass of ChronosBase will automatically have all of the same properties
    as its underlying protobuf class."""
    #pylint: disable=E1101,W0640
    def __init__(self):
        self.proto = self.Proto()
        for field in self.Proto.DESCRIPTOR.fields:
            prop = property(lambda self, name=field.name: getattr(self.proto, name),
                            lambda self, val, name=field.name: setattr(self.proto, name, val),
                            lambda self, name=field.name: delattr(self.proto, name))
            setattr(type(self), field.name, prop)
        for enum in self.Proto.DESCRIPTOR.enum_types:
            for value in enum.values:
                setattr(type(self), value.name, value.number)


class ChronosMeta(type):
    """Provides common functionality for Chronos meta classes, AggregateMeta and EventMeta.
    ChronosMeta ensures that classes have a 'Proto' attribute, and that the attribute is
    a protobuf class. As a convenience, ChronosMeta also adds a 'fullName' class member
    for use elsewhere in Chronos::Core."""
    def __init__(cls, name, bases, dct):
        super(ChronosMeta, cls).__init__(name, bases, dct)
        if len(bases) != 1 or type(bases[0]) != type(ChronosBase):
            if not hasattr(cls, 'Proto'):
                raise ChronosSemanticException('Chronos class must have Proto attribute')
            proto = cls.Proto
            if not issubclass(proto, message.Message):
                raise ChronosSemanticException('Chronos class Proto attribute must be a protocol buffer')
            cls.fullName = '{0}.{1}'.format(cls.__module__, cls.__name__)


class AggregateMeta(ChronosMeta):
    """Validates and sets up classes as proper Chronos Aggregates. Defining an AggregateMeta
    will enforce that only a single AggregateMeta exists in the current module, and then will
    modify the module in preparation for event registration.

    In addition, if an IndexKey property exists on the class, it will be validated against the
    underlying protobuf class members, and class members will be set accordingly on the
    Aggregate class.

    For example, consider the following Protobuf message:
    @code
    message AProtobufMessage {
        optional string firmName = 1;
        optional string infoSysName = 2;
        optional string traderAcronym = 3;
    }
    @endcode

    If the following module is declared:
    @code

    class AnIndex(ChronosIndex):
        firmName = Column(String)
        infoSysName = Column(String)
        constraints = [UniqueConstraint('firmName', 'infoSysName', name='ux_1')]

    class AnAggregate(Aggregate):
        Proto = AProtobufMessage
        Index = AnIndex
    @endcode

    Then Chronos will automatically generate the necessary metadata to allow for querying of
    AnAggregate by firmName and infoSysName. The provided unique constraint will also be enforced
    for both aggregate creation and modification."""
    def __init__(cls, name, bases, dct):
        super(AggregateMeta, cls).__init__(name, bases, dct)
        if hasattr(sys.modules[cls.__module__], 'AggregateClass') and sys.modules[cls.__module__].AggregateClass.__name__ != name:
            raise ChronosSemanticException('Cannot define more than one Aggregate per module')
        sys.modules[cls.__module__].AggregateClass = cls
        sys.modules[cls.__module__].AggregateRegistry = {}
        cls.Dependencies = set() #pylint: disable=C0103
        cls.IndexedAttributes = set() #pylint: disable=C0103
        if hasattr(cls, 'Index'):
            AggregateMeta._setupIndex(cls)
        if not isinstance(cls.Expiration, int):
            raise ChronosSemanticException('Aggregate.Expiration must be an int')
        if cls.Expiration < 0:
            raise ChronosSemanticException('Aggregate.Expiration cannot be negative')

    @staticmethod
    def _setupIndex(kls):
        if not issubclass(kls.Index, ChronosIndex):
            raise ChronosSemanticException('Invalid Index: {0} is not a subclass of ChronosIndex'.format(kls.Index.__name__))
        for indexedAttribute in kls.Index.IndexedAttributes:
            if not hasattr(kls.Proto, indexedAttribute):
                raise ChronosSemanticException('Invalid Index property: {0}'.format(indexedAttribute))
        kls.IndexedAttributes = kls.Index.IndexedAttributes
        kls.IndexStringFormat = '.'.join(sorted('{' + attr + '}' for attr in kls.IndexedAttributes))
        if kls.IndexStringFormat:
            kls.IndexStringFormat = '.' + kls.IndexStringFormat


class EventMeta(ChronosMeta):
    """Validates and sets up classes as proper Chronos Events. Defining an EventMeta
    will enforce that the Event specifies a proper Aggregate and that the Event implements
    required functionality for Chronos processing."""
    def __init__(cls, name, bases, dct):
        super(EventMeta, cls).__init__(name, bases, dct)
        if len(bases) != 1 or type(bases[0]) != type(ChronosBase):
            if not hasattr(cls, 'Aggregate'):
                raise ChronosSemanticException('Chronos Event must have Aggregate attribute')
            aggregate = cls.Aggregate
            if not issubclass(aggregate, Aggregate) or aggregate is Aggregate:
                raise ChronosSemanticException('Chronos Event Aggregate attribute must be a subclass of Aggregate')

            if not hasattr(cls, 'RaiseFor'):
                raise ChronosSemanticException('Chronos Event must define method RaiseFor(self, aggregate)')
            raiseFor = cls.RaiseFor
            def WrappedRaiseFor(self, agg):
                if not isinstance(agg, self.Aggregate):
                    raise ChronosCoreException('Attempted to raise {0} on a type other than {1}'.format(
                        name, aggregate.__name__))
                raiseFor(self, agg)
            cls.Dependencies = [] #pylint: disable=C0103
            cls.RaiseFor = WrappedRaiseFor #pylint: disable=C0103
            sys.modules[cls.__module__].AggregateRegistry[cls.Proto.__name__] = cls


class Aggregate(ChronosBase):
    """The public Aggregate base class. This mainly exists to connect AggregateMeta and ChronosBase
    so that the client can subclass a single class in their code."""
    __metaclass__ = AggregateMeta
    CacheSize = 1000
    Expiration = 0
    def __init__(self, aggregateId, version, expiration):
        super(Aggregate, self).__init__()
        self.aggregateId = aggregateId
        self.version = version
        self.expiration = expiration
        self.lock = threading.Lock()

    def IsValid(self):
        raise NotImplementedError('IsValid')

    def _getAttributeForIndexing(self, attr):
        if not self.proto.HasField(attr):
            return None
        return str(getattr(self, attr))

    def GetIndices(self):
        return dict([(attr, self._getAttributeForIndexing(attr)) for attr in self.IndexedAttributes]) #pylint: disable=E1101

    def HasDivergedFrom(self, previousVersion):
        return self.GetIndices() != previousVersion.GetIndices()

    def ToDict(self):
        return {'aggregateId': self.aggregateId, 'version': self.version,
                'expiration': self.expiration, 'proto': self.proto.SerializeToString()}

    @classmethod
    def FromDict(cls, dct):
        aggregate = cls(long(dct['aggregateId']), long(dct['version']), long(dct['expiration']))
        aggregate.proto.ParseFromString(dct['proto'])

        return aggregate

    def __deepcopy__(self, memo):
        result = type(self)(self.aggregateId, self.version, self.expiration)
        result.proto = copy.deepcopy(self.proto, memo)
        return result

    def __getstate__(self):
        """For queries, Aggregate instances must be pickleable.
        Because threading.Lock cannot be pickled, we have to remove it before returning the dict.
        """
        outputDict = self.__dict__.copy()
        if 'lock' in outputDict:
            del outputDict['lock']
        return outputDict


class Event(ChronosBase):
    """The public Event base class. This mainly exists to connect EventMeta and ChronosBase
    so that the client can subclass a single class in their code."""
    __metaclass__ = EventMeta
    def __init__(self, version, logicVersion):
        super(Event, self).__init__()
        self.version = version
        self.logicVersion = logicVersion

    def ToProto(self, receivedTimestamp, processedTimestamp):
        #pylint: disable=E1101
        proto = EventProto()
        proto.type = self.fullName
        proto.version = self.version
        proto.logicVersion = self.logicVersion
        proto.proto = self.proto.SerializeToString()
        proto.receivedTimestamp = receivedTimestamp
        proto.processedTimestamp = processedTimestamp

        return proto

    @staticmethod
    def FromProtoString(string):
        proto = EventProto()
        proto.ParseFromString(string)

        return proto

class SqliteIndex(object):
    """Abstracts away the details of the sqlite index implementation.
    This class mainly exists to provide the same transactional interface provided by
    AggregateRepository and EventProcessor.
    """
    def __init__(self, sqliteSession):
        self.sqliteSession = sqliteSession

    def Begin(self):
        self.sqliteSession.begin_nested()

    def Commit(self):
        self.sqliteSession.commit()

    def Rollback(self):
        self.sqliteSession.rollback()

    def Close(self):
        self.sqliteSession.close()

    def Add(self, obj):
        self.sqliteSession.add(obj)

    def Flush(self):
        self.sqliteSession.flush()

class IndexStore(object):
    """Handles all interaction relating to Chronos indexing functionality.
    The IndexStore interfaces with sqlite databases instances that are persisted on disk as part
    of the Chronos running configuration. In production, these databases (and the entire Chronos running configuration)
    are persisted on network storage (the Nimble device, currently) in case of individual server failure.

    Instances of this class are not thread-safe."""
    SchemaLock = Lock()
    SQLitePathTemplate = 'sqlite:////var/lib/chronos/index.alembic/db/{0}.sqlite'
    def __init__(self, aggregateClass):
        self.aggregateClass = aggregateClass
        self.indexClass = aggregateClass.Index
        self.tagClass = self.indexClass.CreateTagModel()
        self.checkpointClass = self.indexClass.CreateCheckpointModel()
        self.sqlitePath = self.SQLitePathTemplate.format(self.aggregateClass.__name__)
        self.engine = self._getEngine()
        self.readEngine = create_engine(self.sqlitePath)
        self.sessionMaker = sessionmaker(bind=self.engine)
        self.readSessionMaker = sessionmaker(bind=self.readEngine)

    def Dispose(self):
        self.sessionMaker.close_all()
        self.readSessionMaker.close_all()
        self.engine.dispose()
        self.readEngine.dispose()

    def _getEngine(self):
        engine = create_engine(self.sqlitePath)
        @event.listens_for(engine, "connect")
        def DoConnect(connection, _): #pylint: disable=W0612
            connection.isolation_level = None
        @event.listens_for(engine, "begin")
        def DoBegin(connection): #pylint: disable=W0612
            connection.execute("BEGIN")
        return engine

    def GetSession(self):
        return SqliteIndex(self.sessionMaker())

    def _getReadSession(self):
        return self.readSessionMaker()

    def GetCheckpoint(self, session):
        """Retrieves the current checkpoint information from the SQLite instance.
        @param session A SqliteIndex instance that will be used to retrieve the checkpoint.
        @returns None If no checkpoint information has been persisted."""
        checkpoint = session.sqliteSession.query(self.checkpointClass).get(1)
        return checkpoint

    def UpdateCheckpoint(self, maxRequestId, session):
        """Persists the provided maxRequestId into the SQLite instance's checkpoint.
        @param maxRequestId The maximum requestId that has been persisted to Redis.
        @param session The SqliteIndex instance that should be used for the operation."""
        checkpoint = session.sqliteSession.query(self.checkpointClass).get(1)
        if checkpoint is None:
            checkpoint = self.checkpointClass()
        checkpoint.maxRequestId = maxRequestId
        session.Add(checkpoint)
        session.Flush()

    def ReindexAggregate(self, aggregate, session):
        """Upserts the provided aggregate's information into the underlying sqlite database.
        @param aggregate The aggregate instance that should be reindexed.
        @param session The SqliteIndex instance that should be used for the operation."""
        if not self.aggregateClass.IndexedAttributes:
            return

        indices = aggregate.GetIndices()
        index = session.sqliteSession.query(self.indexClass).get(aggregate.aggregateId)
        if index is None:
            index = self.indexClass(aggregateId=aggregate.aggregateId, **indices)
        else:
            for attr, value in indices.iteritems():
                setattr(index, attr, value)
        session.Add(index)
        session.Flush()

    def IndexTag(self, tag, aggregateId, session):
        """Persists the provided tag information to the SQLite instance.
        @param tag The tag to be indexed. This must be unique within the context of the Aggregate instance.
        @param session The SqliteIndex instance that should be used for the operation."""
        date = long(time.time() * 1e9)
        tagModel = self.tagClass(tag=tag, aggregateId=int(aggregateId), createDate=date)
        session.Add(tagModel)
        session.Flush()
        return date

    def GetIndexRow(self, aggregateId):
        session = self._getReadSession()
        row = session.query(self.indexClass).get(aggregateId)
        session.close()
        return row

    def GetTagRow(self, aggregateId, tag):
        session = self._getReadSession()
        row = session.query(self.tagClass).filter_by(aggregateId=aggregateId, tag=tag).one_or_none()
        session.close()
        return row

    def GetTagsByAggregateId(self, aggregateId):
        session = self._getReadSession()
        query = session.query(self.tagClass).filter_by(aggregateId=aggregateId).all()
        session.close()
        return query

    def GetAllTags(self):
        session = self._getReadSession()
        query = session.query(self.tagClass).all()
        session.close()
        return query

    def MarkTagsAsArchived(self, tags):
        """Updates the provided tags to mark them as archived (no longer available via Redis).
        @param tags A list of tags to be marked. If any tag provided does not have a corresponding row
        in the database, one will be created for it."""
        databaseSession = self.GetSession()
        for tagName, tag in tags:
            tagRow = databaseSession.sqliteSession.query(self.tagClass).filter_by(tag=tagName, aggregateId=tag['aggregateId']).one()
            if tagRow is None:
                EventLogger.LogWarningAuto(self, 'Creating index for tag', 'Attempted to mark unknown tag as archived',
                                           tags={'Aggregate': self.aggregateClass.__name__, 'Tag': tagName})
                tagRow = self.tagClass(tag=tagName, aggregateId=tag['aggregateId'], createDate=tag['createDate'])
            tagRow.isArchived = True
            databaseSession.Add(tagRow)
        databaseSession.Commit()
        databaseSession.Close()

    def MarkAsArchived(self, aggregateIds):
        """Updates the provided aggregate to mark it as archived.
        This action doesn't alter any index attributes, but signifies that the aggregate's information
        is now stored in a SQL Server instance rather than in Redis.
        @param aggregateIds A list of aggregateIds to be marked. If any aggregateId provided does not have a corresponding
        row in the database, a warning will be logged and that single operation will be skipped."""
        databaseSession = self.GetSession()
        for aggregateId in aggregateIds:
            aggregateId = long(aggregateId)
            aggregate = databaseSession.sqliteSession.query(self.indexClass).get(aggregateId)
            if aggregate is None:
                EventLogger.LogWarningAuto(self, 'Attempted to mark unknown aggregate as archived',
                                           tags={'Aggregate': self.aggregateClass.__name__, 'AggregateId': aggregateId})
                continue
            aggregate.isArchived = True
            databaseSession.Add(aggregate)
        databaseSession.Commit()
        databaseSession.Close()

    def UpdateMaxArchivedVersion(self, aggregateId, maxVersion):
        """Updates the provided aggregate with its maximum archived version.
        This action doesn't alter any index attributes, but signifies that the aggregate's event
        information is now stored in a SQL Server instance rather than in Redis.
        @param aggregateId The aggregateId of the instance which has archived events.
        @param maxVersion The maximum event version that has been removed from Redis."""
        databaseSession = self.GetSession()
        aggregateId = long(aggregateId)
        aggregate = databaseSession.sqliteSession.query(self.indexClass).get(aggregateId)
        aggregate.maxArchivedVersion = maxVersion
        databaseSession.Add(aggregate)
        databaseSession.Commit()
        databaseSession.Close()

    def RetrieveAggregateIds(self, isArchived=False, **indices):
        """Returns a list of all aggregateIds that satisfy the provided index properties.
        This method will raise an Exception if a non-indexed property is supplied as a keyword argument.
        @param isArchived A flag indicating whether archived or live instances should be searched.
        @param indices Kwargs representing the indexed values that should be used in the lookup.
        @throws ChronosCoreException If the Aggregate is not indexed."""
        if not self.aggregateClass.IndexedAttributes:
            raise ChronosCoreException('Aggregate {0} is not indexed'.format(self.aggregateClass.__name__))
        session = self._getReadSession()
        query = session.query(self.indexClass).filter_by(**indices)
        if isArchived is not None:
            query = query.filter_by(isArchived=isArchived)
        session.close()
        return [row.aggregateId for row in query]

    def RetrieveAggregates(self, fromAggregateId, returnCount, isArchived=False, **indices):
        """Returns a list of all aggregates and their index properties that satisfy the provided index properties.
        This method will raise an Exception if a non-indexed property is supplied as a keyword argument.
        @param isArchived A flag indicating whether archived or live instances should be searched.
        @param indices Kwargs representing the indexed values that should be used in the lookup.
        @throws ChronosCoreException If the Aggregate is not indexed."""
        if not self.aggregateClass.IndexedAttributes:
            raise ChronosCoreException('Aggregate {0} is not indexed'.format(self.aggregateClass.__name__))
        session = self._getReadSession()
        query = session.query(self.indexClass).filter_by(**indices)
        if fromAggregateId is not None:
            query = query.filter(self.indexClass.aggregateId <= fromAggregateId)
        if isArchived is not None:
            query = query.filter_by(isArchived=isArchived)
        if returnCount is not None:
            query = query.limit(returnCount)
        session.close()
        return [aggregate for aggregate in query]

    @staticmethod
    def UpdateIndexSchema(aggregateClass):
        """Executes an external procedure that updates the underlying sqlite database using an on-disk alembic configuration.
        This method relies on the Chronos running configuration being properly configured.
        @param aggregateClass The Aggregate subclass that should have its new schema applied to its SQLite instance."""
        if not hasattr(aggregateClass, 'Index'):
            raise ChronosCoreException('Aggregate class must define index')
        env = os.environ.copy()
        env['AGGREGATE_NAME'] = aggregateClass.__name__
        with IndexStore.SchemaLock:
            process = subprocess.Popen(['/var/lib/chronos/run_migration'], env=env, cwd='/var/lib/chronos',
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            if process.returncode:
                EventLogger.LogError('Chronos.Core', 'IndexStore', 'UpdateIndexSchema', 'Error updating index schema',
                                     'External process had non-zero return code',
                                     tags={'Stdout': stdout, 'Stderr': stderr, 'ReturnCode': process.returncode,
                                           'Aggregate': aggregateClass.__name__})
                raise ChronosCoreException('Unable to update index schema')

    @staticmethod
    def GetIndexStore(aggregateName, eventStore, logicCompiler):
        aggregateLogic = eventStore.GetLatestAggregateLogicByName(aggregateName)
        module = logicCompiler.BuildModule(aggregateName, aggregateLogic)
        return IndexStore(module.AggregateClass)

class ChronosCoreProvider(object):
    """A factory class that provides implementations of core Chronos functionality
    to other modules.

    @see Chronos::Gateway"""
    def __init__(self, aggregateClass):
        self.infrastructureProvider = InfrastructureProvider()
        self.indexStore = IndexStore(aggregateClass)
        self.eventStore = self.infrastructureProvider.GetConfigurablePlugin(ConfigurablePlugin.EventStore)
        self.aggregateRepository = AggregateRepository(aggregateClass, self.indexStore, self.eventStore)
        self.eventProcessor = EventProcessor(self.eventStore, AggregateLogicCompiler(self.eventStore))

    def Dispose(self):
        self.indexStore.Dispose()
        self.eventStore.Dispose()

    def GetIndexStore(self):
        return self.indexStore

    def GetRepository(self):
        return self.aggregateRepository

    def GetProcessor(self):
        return self.eventProcessor


class AggregateRepository(object):
    """The in-memory representation of the current state of all aggregates within an
    aggregate root. AggregateRepository handles interaction with an event store
    to return aggregates to the user."""
    def __init__(self, aggregateClass, indexStore, eventStore):
        if not issubclass(aggregateClass, Aggregate):
            raise ChronosCoreException('aggregateClass must subclass Aggregate')
        self.aggregateClass = aggregateClass
        self.indexStore = indexStore
        self.eventStore = eventStore
        self.repository = LRUEvictingMap(capacity=aggregateClass.CacheSize)
        self.transactionRepository = None
        self.transactionLock = threading.Lock()
        self.transactionEvent = threading.Event()
        self.transactionEvent.set()

    def _isTransactionInProgress(self):
        return self.transactionRepository is not None

    def Begin(self):
        with self.transactionLock:
            if self._isTransactionInProgress():
                return
            self.transactionRepository = {}
            self.transactionEvent.clear()

    def Commit(self):
        with self.transactionLock:
            if not self._isTransactionInProgress():
                return
            self.repository.update(self.transactionRepository)
            self.transactionRepository = None
            self.transactionEvent.set()

    def Rollback(self):
        with self.transactionLock:
            if not self._isTransactionInProgress():
                return
            self.transactionRepository = None
            self.transactionEvent.set()

    def Create(self):
        """Creates a new uninitialized aggregate, permanently incrementing the aggregate root id
        in the event store. This method does not actually add the aggregate into the repository;
        the aggregate should be emplaced into the repository once the creation event is applied."""
        aggregateId = self.eventStore.GetAndIncrementAggregateId(self.aggregateClass)
        expiration = 0
        if self.aggregateClass.Expiration:
            expiration = long(time.time() * 1e9) + long(self.aggregateClass.Expiration * 1e9)
        aggregate = self.aggregateClass(aggregateId, long(1), expiration)

        return aggregate

    def Get(self, aggregateId):
        """Retrieves a single aggregate snaphost using the provided id."""
        if aggregateId < 1:
            raise ChronosCoreException('aggregateId must be greater than zero')

        if self._isTransactionInProgress():
            try:
                return self.transactionRepository[aggregateId]
            except KeyError:
                pass
        aggregate = self.repository.get(aggregateId, None)
        if aggregate is None:
            aggregate = self._getAggregateSnapshot(aggregateId)

        return aggregate

    def AtomicGet(self, aggregateId):
        """Retrieves a single aggregate snapshot for dependency acquisition.
        This method differs from Get by checking the current transactional status
        of the AggregateRepository instance and ensuring that the snapshot is not
        returned in a state that could lead to inconsistent access.
        """
        with self.transactionLock:
            if not self._isTransactionInProgress() or aggregateId not in self.transactionRepository:
                return self.Get(aggregateId)
        self.transactionEvent.wait()
        return self.AtomicGet(aggregateId)

    def Emplace(self, aggregate):
        """Replaces an existing aggregate with its new state."""
        with self.transactionLock:
            if self._isTransactionInProgress():
                self.transactionRepository[aggregate.aggregateId] = aggregate
            else:
                self.repository[aggregate.aggregateId] = aggregate

    def _getAggregateSnapshot(self, aggregateId):
        aggregate = self.eventStore.TryGetSnapshot(self.aggregateClass, aggregateId)
        if aggregate is None:
            raise ChronosCoreException('Unknown aggregateId {0} for {1}'.format(aggregateId, self.aggregateClass.fullName))
        self.repository[aggregateId] = aggregate
        return aggregate

    def GetEventPersistenceCheckpoint(self):
        return self.eventStore.GetEventPersistenceCheckpoint(self.aggregateClass)

    def GetAll(self):
        """Retrieves all aggregate snapshots available in the event store and returns
        them. This does not affect the current in-memory representation."""
        return self.eventStore.GetAllSnapshots(self.aggregateClass)

    def GetFromIndex(self, **kwargs):
        """Retrieves all aggregates that satisfy the provided index contraints."""
        aggregateIds = self.indexStore.RetrieveAggregateIds(**kwargs)
        return self.eventStore.GetIndexedSnapshots(self.aggregateClass, aggregateIds)

    def GetTag(self, aggregateId, tag):
        return self.eventStore.GetTag(self.aggregateClass, aggregateId, tag)


class AggregateLogicCompiler(object):
    """Handles all operations related to Aggregate logic compilation.
       Aggregate logic is compiled into Python modules that are used by ChronosProcess.
       In addition, this class handles the persistence of logic versions and ids so that
       events from the past can be replayed without worrying about backwards compatibility
       while making changes."""
    def __init__(self, eventStore):
        self.eventStore = eventStore

    def BuildModule(self, aggregateName, aggregateLogic):
        """Constructs a module containing the provided aggregateLogic, wired to the
        proper Protobuf entities."""
        protoFilePath = self._writeFilesForRegistration(aggregateName, aggregateLogic)
        self._compileProtobufContracts(protoFilePath, aggregateName)
        self.ImportAggregateLogic('{0}_pb2'.format(aggregateName))
        return self.ImportAggregateLogic(aggregateName)

    def Compile(self, aggregateName, aggregateLogic):
        """Handles compilation and persistence of Aggregate logic.
        If the provided aggregateLogic differs from the previously compiled
        logic (or no previously provided logic could be found), the logic
        will be persisted and the version incremented."""
        module = self.BuildModule(aggregateName, aggregateLogic)
        logicVersion = self._getLogicVersionWithPersistence(module.AggregateClass, aggregateLogic)

        return logicVersion, module

    def _getLogicVersionWithPersistence(self, aggregateClass, aggregateLogic):
        currentLogicDict = self.eventStore.GetLatestAggregateLogic(aggregateClass)
        if currentLogicDict is not None:
            currentLogicVersion, currentLogic = currentLogicDict.items()[0]
        if currentLogicDict is None or (currentLogic.pythonFileContents != aggregateLogic.pythonFileContents
                                        or currentLogic.protoFileContents != aggregateLogic.protoFileContents):
            IndexStore.UpdateIndexSchema(aggregateClass)
            if currentLogicDict is None:
                message = 'No existing logic found' #pylint: disable=W0621
            else:
                message = 'Registered logic and existing logic differ'

            EventLogger.LogInformationAuto(self, 'Persisting aggregate logic', message,
                                           tags={'Aggregate': aggregateClass.__name__})
            return self.eventStore.PersistAggregateLogic(aggregateClass, aggregateLogic)
        else:
            EventLogger.LogInformationAuto(self, 'Existing aggregate logic found',
                                           tags={'LogicVersion': currentLogicVersion})
            return currentLogicVersion

    @staticmethod
    def _writeFilesForRegistration(aggregateName, aggregateLogic):
        for pycFile in glob.glob('{0}*.pyc'.format(aggregateName)):
            os.remove(pycFile)
        pythonFilePath = '{0}.py'.format(aggregateName)
        protoFilePath = '{0}.proto'.format(aggregateName)
        with open(pythonFilePath, 'w') as pythonFile:
            pythonFile.write(aggregateLogic.pythonFileContents)
        with open(protoFilePath, 'w') as protoFile:
            protoFile.write(aggregateLogic.protoFileContents)

        return protoFilePath

    def _compileProtobufContracts(self, protoFilePath, aggregateName):
        protoc = subprocess.Popen('protoc --python_out=. {0}'.format(protoFilePath), shell=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = protoc.communicate()
        if protoc.returncode != 0:
            errorMessage = 'Error compiling protobuf file'
            EventLogger.LogErrorAuto(self, errorMessage,
                                     tags={'ReturnCode': protoc.returncode, 'Aggregate': aggregateName,
                                           'StdOut': out, 'StdErr': err})
            raise ChronosCoreException(errorMessage)

    @staticmethod
    def ImportAggregateLogic(aggregateName):
        impfile = None
        try:
            impfile, pathname, desc = imp.find_module(aggregateName)
            return imp.load_module(aggregateName, impfile, pathname, desc)
        finally:
            if impfile:
                impfile.close()

class NoEventsFound(Exception):
    pass

def SerializeBufferItem(bufferItem):
    return bufferItem.Serialize()

class EventProcessor(object):
    """Handles processing and replay of events against aggregate instances.
    This class is thread-safe."""
    MaxBufferedEvents = 15
    def __init__(self, eventStore, logicCompiler, persistencePool=None):
        if persistencePool is None:
            persistencePool = Pool(processes=4)
        self.eventStore = eventStore
        self.logicCompiler = logicCompiler
        self.persistenceBuffer = []
        self.persistencePool = persistencePool
        self.lock = threading.RLock()
        self.isReplaying = False
        self.transactionBuffer = None

    def _isTransactionInProgress(self):
        return self.transactionBuffer is not None

    def Begin(self):
        if self._isTransactionInProgress():
            return
        self.transactionBuffer = []

    def Commit(self):
        if not self._isTransactionInProgress():
            return
        results = [self.persistencePool.apply_async(func=SerializeBufferItem, args=(item,)) for item in self.transactionBuffer]
        self.persistenceBuffer.extend(results)
        self.transactionBuffer = None

    def Rollback(self):
        if not self._isTransactionInProgress():
            return
        self.transactionBuffer = None

    def Process(self, request, aggregate, singleEvent):
        """Applies the provided event to the provided aggregate. If the EventProcessor
        is not replaying, the action will be persisted in the event store and all
        subscribed clients will be notified of the event."""
        if aggregate.version != singleEvent.version:
            raise ChronosCoreException('Cannot apply event with mismatching version {0} {1}'.format(aggregate.version, singleEvent.version))

        with self.lock:
            singleEvent.RaiseFor(aggregate)
            aggregate.version += 1
            if not aggregate.IsValid() and not self.isReplaying:
                EventLogger.LogWarningAuto(self, 'Aborting event processing', 'Aggregate.IsValid returned false',
                                           tags={'RequestId': request.requestId, 'AggregateId': aggregate.aggregateId})
                raise ChronosCoreException('Aggregate was put into an invalid state')
            if self.isReplaying:
                bufferItem = None
            else:
                bufferItem = PersistenceBufferItem(aggregate.__class__, request, aggregate, singleEvent,
                                                   request.receivedTimestamp, long(time.time() * 1e9))
            return aggregate, bufferItem

    def _internalPersist(self, bufferItem):
        if self._isTransactionInProgress():
            self.transactionBuffer.append(bufferItem)
        else:
            result = self.persistencePool.apply_async(func=SerializeBufferItem, args=(bufferItem,))
            self.persistenceBuffer.append(result)

    def ProcessIndexDivergence(self, aggregateClass, aggregateId):
        """Notifies clients that an aggregate instance has diverged indices."""
        managementNotification = ChronosManagementNotification(notificationType=ChronosManagementNotification.INDEX_DIVERGED, #pylint: disable=E1101
                                                               aggregateId=aggregateId)
        bufferItem = PersistenceBufferManagementItem(aggregateClass, managementNotification)
        self._internalPersist(bufferItem)

    def ProcessFailure(self, request, aggregateClass, exception):
        """Notifies clients that an event processing request has failed."""
        bufferItem = PersistenceBufferFailureItem(aggregateClass, request, exception)
        self._internalPersist(bufferItem)


    def ProcessTag(self, aggregate, tag, tagExpiration, createDate=long(time.time())):
        bufferItem = PersistenceBufferTagItem(aggregate.__class__, aggregate, tag, tagExpiration, createDate)
        self._internalPersist(bufferItem)

    def EnqueueForPersistence(self, bufferItem):
        self._internalPersist(bufferItem)

    def FlushPersistenceBuffer(self, aggregateClass, shouldForce=False):
        if self._isTransactionInProgress():
            raise ChronosCoreException('Cannot flush persistence buffer while transaction is in progress')
        with self.lock:
            if shouldForce or len(self.persistenceBuffer) >= self.MaxBufferedEvents:
                self.eventStore.PersistEvents(aggregateClass, self.persistenceBuffer)
                del self.persistenceBuffer[:]
                return True
            return False

    def _applyEventProtos(self, aggregate, eventProtos):
        """Applies all events later than the current version of the supplied aggregate
        to that same instance."""
        logicVersions = list(set(event.logicVersion for event in eventProtos))
        logicResults = self.eventStore.GetAggregateLogic(aggregate.__class__, logicVersions)
        logicDict = dict(zip(logicVersions, logicResults))

        logicVersion = eventProtos[0].logicVersion
        module = self.logicCompiler.BuildModule(aggregate.__class__.__name__, logicDict[logicVersion])
        for eventProto in eventProtos:
            if eventProto.logicVersion != logicVersion:
                logicVersion = eventProto.logicVersion
                module = self.logicCompiler.BuildModule(aggregate.__class__.__name__, logicDict[logicVersion])

            _, className = eventProto.type.rsplit('.', 1)
            eventClass = getattr(module, className)
            singleEvent = eventClass(eventProto.version, eventProto.logicVersion)
            singleEvent.proto.ParseFromString(eventProto.proto)
            singleEvent.receivedTimestamp = eventProto.receivedTimestamp
            singleEvent.processedTimestamp = eventProto.processedTimestamp

            tempAggregate = singleEvent.Aggregate(aggregate.aggregateId, aggregate.version, aggregate.expiration)
            tempAggregate.proto.ParseFromString(aggregate.proto.SerializeToString())
            self.Process(None, tempAggregate, singleEvent) #pylint: disable=E1120
            aggregate.proto.ParseFromString(tempAggregate.proto.SerializeToString())
            aggregate.version = tempAggregate.version

    def ReplayByTimestampRange(self, aggregate, fromTimestamp, toTimestamp):
        """Applies all events processed later than the fromTimestamp and before the toTimestamp
        of the supplied aggregate to the same instance"""
        with self.lock:
            try:
                self.isReplaying = True
                eventProtos = self.eventStore.GetEventsByTimestampRange(aggregate, fromTimestamp, toTimestamp)
                if len(eventProtos) == 0:
                    raise NoEventsFound('No events found from timestamps {0} to {1}'.format(fromTimestamp, toTimestamp))

                self._applyEventProtos(aggregate, eventProtos)
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Replay of aggregate failed',
                                             tags={'Aggregate': aggregate.__class__.__name__,
                                                   'AggregateId': aggregate.aggregateId,
                                                   'FromTimestamp': fromTimestamp,
                                                   'ToTimestamp': toTimestamp})
            finally:
                self.isReplaying = False

    def ReplayToVersion(self, aggregate, toVersion):
        """Applies all events later than the current version of the supplied aggregate
        to that same instance."""
        with self.lock:
            try:
                self.isReplaying = True
                eventProtos = self.eventStore.GetEventsToVersion(aggregate, toVersion)
                if len(eventProtos) == 0:
                    raise NoEventsFound('No events found from version {0} to version {1}'.format(aggregate.version, toVersion))

                self._applyEventProtos(aggregate, eventProtos)
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Replay of aggregate failed',
                                             tags={'Aggregate': aggregate.__class__.__name__,
                                                   'AggregateId': aggregate.aggregateId,
                                                   'Version': aggregate.version,
                                                   'ToVersion': toVersion})
            finally:
                self.isReplaying = False
