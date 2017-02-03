#pylint: disable=C0302
"""@addtogroup Chronos
@{
"""
import copy
import glob
import imp
import json
import os
import pickle
import re
import subprocess
import sys
import threading
import time
from abc import ABCMeta, abstractmethod
from collections import namedtuple

from google.protobuf import message, json_format

from Chronos.EventLogger import EventLogger
from Chronos.Map import LRUEvictingMap
from Chronos.Chronos_pb2 import EventProto, ChronosManagementNotification, ChronosResponse


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


class ChronosFailedEventPersistenceException(Exception):
    """An Exception indicating that the events failed to be persisted to either the notifier or the database; likely as a result of a connection loss."""
    pass


class ChronosConstraint(object):
    """Provides ensurance that all ChronosConstraints have a unique (and non-None) name to identify them by.
    All Constraints must inherit from ChronosConstraint
    """
    __metaclass__ = ABCMeta
    def __init__(self, *args, **kwargs):
        try:
            self.name = kwargs['name']
        except KeyError:
            raise ChronosCoreException('Not given name for {}. Constraints must be given a name'.format(self.__class__.__name__))
        if self.name is None:
            raise ChronosCoreException('Given None constraint name. Constraint names must be unique, non-None, and contain only alphanumeric characters and underscores')
        self.validName = re.compile(r'^[a-zA-Z0-9_]+$')
        if not self._isValid(self.name):
            raise ChronosCoreException(
                'Given invalid constraint name {0}.Constraint names must be unique, non-None, and contain only alphanumeric characters and underscores'.format(self.name))
        self.attributes = set(args)

    def _isValid(self, name):
        return self.validName.match(name) # only alphanumeric and underscores allowed

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return set(self.attributes) == set(other.attributes) and self.name == other.name
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @abstractmethod
    def Create(self, aggregateName, databaseConnection):
        pass

    @abstractmethod
    def Drop(self, aggregateName, databaseConnection):
        pass


class Unique(ChronosConstraint):
    """Provides the ability to add unique constraints on the attributes provided. If more than one attribute is passed, the unique constraint will be
    on the composition of the keys. If it is desired to have multiple fields be unique individually, a Unique must be created for each attribute specifically.
    """
    def __init__(self, *args, **kwargs):
        super(Unique, self).__init__(*args, **kwargs)

    def Create(self, aggregateName, databaseConnection):
        databaseConnection.CreateUniqueConstraint(aggregateName, self.name, self.attributes)

    def Drop(self, aggregateName, databaseConnection):
        databaseConnection.DropIndex(self.name)


class Index(ChronosConstraint):
    """Speeds up index queries on the specfied attribute. """
    def __init__(self, *args, **kwargs):
        super(Index, self).__init__(*args, **kwargs)

    def Create(self, aggregateName, databaseConnection):
        databaseConnection.CreateIndex(aggregateName, self.name, self.attributes)

    def Drop(self, aggregateName, databaseConnection):
        databaseConnection.DropIndex(self.name)


class NonNull(ChronosConstraint):
    """Requires that the values associated with the specified attributes be non-null"""
    def __init__(self, *args, **kwargs):
        super(NonNull, self).__init__(*args, **kwargs)

    def Create(self, aggregateName, databaseConnection):
        databaseConnection.CreateNonNullConstraint(aggregateName, self.name, self.attributes)

    def Drop(self, aggregateName, databaseConnection):
        databaseConnection.DropConstraint(aggregateName, self.name)


class NoCase(ChronosConstraint):
    """Ensures case insensitivity for specified attributes. """
    def __init__(self, *args, **kwargs):
        super(NoCase, self).__init__(*args, name='*', **kwargs)

    def _isValid(self, name):
        return True

    def Create(self, aggregateName, databaseConnection):
        pass

    def Drop(self, aggregateName, databaseConnection):
        pass

class ChronosIndexMeta(type):
    def __init__(cls, name, bases, dct):
        super(ChronosIndexMeta, cls).__init__(name, bases, dct)
        cls.IndexedAttributes = set() #pylint: disable=C0103
        cls.Constraints = set() #pylint: disable=C0103
        cls.NoCaseAttributes = set() #pylint: disable=C0103
        try:
            cls.IndexedAttributes = set(dct['attributes'])
            cls.Constraints = set(dct['constraints'])
        except TypeError:
            raise ChronosSemanticException('Ensure that both attributes and constraints are iterable tuples')
        except KeyError:
            pass

        noCaseConstraints = [constraint for constraint in cls.Constraints if isinstance(constraint, NoCase)]
        if len(noCaseConstraints) > 1:
            raise ChronosSemanticException(
                'Only one NoCase constraint can be applied. If you wish to make multiple attributes case insensitve just append them to a single NoCase constraint.')
        if len(noCaseConstraints) == 0:
            return

        cls.NoCaseAttributes = set([attribute for attribute in noCaseConstraints[0].attributes])


class ChronosIndex(object):
    __metaclass__ = ChronosIndexMeta


class ChronosBase(object):
    """Provides common functionality for Chronos base classes, Aggregate and Event.
    Any subclass of ChronosBase will automatically have all of the same properties
    as its underlying protobuf class."""
    #pylint: disable=E1101,W0640
    def __init__(self):
        self.proto = self.Proto()
        for field in self.Proto.DESCRIPTOR.fields_by_name.iterkeys():
            prop = property(lambda self, name=field: getattr(self.proto, name),
                            lambda self, val, name=field: setattr(self.proto, name, val),
                            lambda self, name=field: delattr(self.proto, name))
            setattr(type(self), field, prop)
        for enum in self.Proto.DESCRIPTOR.enum_types:
            for value in enum.values:
                setattr(type(self), value.name, value.number)
        for oneof in self.Proto.DESCRIPTOR.oneofs_by_name.iterkeys():
            prop = property(lambda self, name=oneof: getattr(self.proto, name),
                            lambda self, val, name=oneof: setattr(self.proto, oneof, val),
                            lambda self, name=oneof: delattr(self.proto, name))
            setattr(type(self), oneof, prop)


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
        attributes = ('firmName', 'infoSysName')
        constraints = (Unique('firmName', 'infoSysName', name='ux_1'))

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
        cls.Constraints = set() #pylint: disable=C0103
        if hasattr(cls, 'Index'):
            AggregateMeta._setupIndex(cls)
        else:
            EventLogger.LogErrorAuto(cls, 'Aggregate does not have an Index', tags={'Aggregate': name})

    @staticmethod
    def _setupIndex(kls):
        if not issubclass(kls.Index, ChronosIndex):
            raise ChronosSemanticException('Invalid Index: {0} is not a subclass of ChronosIndex'.format(kls.Index.__name__))
        for indexedAttribute in kls.Index.IndexedAttributes:
            if not indexedAttribute in kls.Proto.DESCRIPTOR.fields_by_name:
                raise ChronosSemanticException('Invalid Index property: {0}'.format(indexedAttribute))
        kls.IndexedAttributes = kls.Index.IndexedAttributes
        kls.Constraints = AggregateMeta._ensureUnique(kls.Index.Constraints)
        kls.NoCaseAttributes = kls.Index.NoCaseAttributes
        kls.IndexStringFormat = '.'.join(sorted('{' + attr + '}' for attr in kls.IndexedAttributes))
        if kls.IndexStringFormat:
            kls.IndexStringFormat = '.' + kls.IndexStringFormat

    @staticmethod
    def _ensureUnique(constraints):
        names = set([constraint.name for constraint in constraints])
        if len(names) != len(constraints):
            raise ChronosSemanticException('Violation of unique constraint name. Found duplicated name in the list of constraint names :{}.'.format(names))
        return constraints


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
    def __init__(self, aggregateId, version):
        super(Aggregate, self).__init__()
        self.aggregateId = aggregateId
        self.version = version
        self.lock = threading.Lock()

    def IsValid(self):
        raise NotImplementedError('IsValid')

    def _getAttributeForIndexing(self, attribute):
        value = str(getattr(self, attribute))
        if attribute in self.NoCaseAttributes:  #pylint: disable=E1101
            value = value.lower()
        return value

    def GetIndices(self):
        return {attribute: self._getAttributeForIndexing(attribute) for attribute in self.IndexedAttributes} #pylint: disable=E1101

    def HasDivergedFrom(self, previousVersion):
        return self.GetIndices() != previousVersion.GetIndices()

    def ToDict(self):
        return {'aggregateId': self.aggregateId, 'version': self.version, 'proto': self.proto.SerializeToString()}

    def ToRESTDict(self):
        return {'aggregateId': self.aggregateId, 'version': self.version,
                'proto':  json.loads(json_format.MessageToJson(self.proto, including_default_value_fields=True))}

    @classmethod
    def FromDict(cls, dct):
        aggregate = cls(long(dct['aggregateId']), long(dct['version']))
        aggregate.proto.ParseFromString(dct['proto'])

        return aggregate

    def __deepcopy__(self, memo):
        result = type(self)(self.aggregateId, self.version)
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
        return EventProto(type=self.fullName, version=self.version, logicVersion=self.logicVersion, proto=self.proto.SerializeToString(),
                          receivedTimestamp=receivedTimestamp, processedTimestamp=processedTimestamp)

    @staticmethod
    def CreateInstance(eventType, eventVersion, logicVersion, proto, receivedTimestamp, processedTimestamp):
        return EventProto(type=eventType, version=eventVersion, logicVersion=logicVersion, proto=str(proto),
                          receivedTimestamp=receivedTimestamp, processedTimestamp=processedTimestamp)


class AggregateRepository(object):
    """The in-memory representation of the current state of all aggregates within an
    aggregate root. AggregateRepository handles interaction with an event store
    to return aggregates to the user."""
    def __init__(self, aggregateClass, eventReader):
        if not issubclass(aggregateClass, Aggregate):
            raise ChronosCoreException('aggregateClass must subclass Aggregate')
        self.aggregateClass = aggregateClass
        self.eventReader = eventReader
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
        aggregateId = self.eventReader.GetNextAggregateId()
        aggregate = self.aggregateClass(aggregateId, long(1))

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
        aggregate = self.eventReader.TryGetSnapshot(aggregateId)
        if aggregate is None:
            raise ChronosCoreException('Unknown aggregateId {0} for {1}'.format(aggregateId, self.aggregateClass.fullName))
        self.repository[aggregateId] = aggregate
        return aggregate

    def GetEventPersistenceCheckpoint(self):
        return self.eventReader.GetEventPersistenceCheckpoint()

    def GetStagedEvents(self):
        return self.eventReader.GetStagedEvents()

    def GetAll(self):
        """Retrieves all aggregate snapshots available in the event store and returns
        them. This does not affect the current in-memory representation."""
        return self.eventReader.GetAllSnapshots()

    def GetFromIndex(self, indexKeys):
        """Retrieves all aggregates that satisfy the provided index contraints."""
        return self.eventReader.GetIndexedSnapshots(indexKeys)

    def GetTag(self, aggregateId, tag):
        return self.eventReader.TryGetTag(aggregateId, tag)


class ConstraintEncoder(json.JSONEncoder):
    def default(self, obj): #pylint: disable=E0202
        if isinstance(obj, (list, dict, str, unicode, int, float, bool, type(None))):
            return json.JSONEncoder.default(self, obj)
        return {'_python_object': pickle.dumps(obj)}

    @staticmethod
    def as_python_object(dct): #pylint: disable=C0103
        if '_python_object' in dct:
            return pickle.loads(str(dct['_python_object']))
        return dct


class ConstraintSerializer(object):
    def __init__(self):
        self.encoder = ConstraintEncoder()

    def Deserialize(self, constraints):
        if constraints is not None:
            return json.loads(constraints, object_hook=self.encoder.as_python_object)

    @staticmethod
    def Serialize(constraints):
        if constraints is None or len(constraints) == 0:
            return None
        return json.dumps(constraints, cls=ConstraintEncoder)


IndexDivergence = namedtuple('IndexDivergence', ['constraintsToAdd', 'constraintsToRemove'])

class ConstraintComparer(object):
    """A Utility class that is responsible for the comparison and updating of constraints and indices necessary when registering new logic."""
    def __init__(self, newConstraints, oldConstraints, serializedNewConstraints, serializedOldConstraints):
        self.newConstraints = newConstraints
        self.oldConstraints = oldConstraints
        self.serializedNewConstraints = serializedNewConstraints
        self.serializedOldConstraints = serializedOldConstraints

    def UpdateConstraints(self):
        if (self.oldConstraints is None and self.newConstraints is None) or self.serializedOldConstraints == self.serializedNewConstraints:
            return IndexDivergence([], [])
        elif self.oldConstraints is None or len(self.oldConstraints) == 0:
            return IndexDivergence(self.newConstraints, [])
        elif self.newConstraints is None or len(self.newConstraints) == 0:
            return IndexDivergence([], self.oldConstraints)
        else:
            return self._handleConstraintDivergence(self.oldConstraints, self.newConstraints)

    @staticmethod
    def _handleConstraintDivergence(oldConstraints, newConstraints):
        old = {constraint.name: constraint for constraint in oldConstraints}
        new = {constraint.name: constraint for constraint in newConstraints}
        newKeys = set(new.iterkeys())
        oldKeys = set(old.iterkeys())
        intersectKeys = newKeys.intersection(oldKeys)
        added = newKeys - oldKeys
        removed = oldKeys - newKeys
        modified = set(x for x in intersectKeys if new[x] != old[x])

        constraintNamesToAdd = added.union(modified)
        constraintsToAdd = [new[name] for name in constraintNamesToAdd]
        constraintNamesToRemove = modified.union(removed)
        constraintsToRemove = [old[name] for name in constraintNamesToRemove]
        return IndexDivergence(constraintsToAdd, constraintsToRemove)


class AggregateLogicCompiler(object):
    """Handles all operations related to Aggregate logic compilation.
       Aggregate logic is compiled into Python modules that are used by ChronosProcess.
       In addition, this class handles the persistence of logic versions and ids so that
       events from the past can be replayed without worrying about backwards compatibility
       while making changes."""
    def __init__(self, logicStore):
        self.logicStore = logicStore

    def BuildModule(self, aggregateName, aggregateLogic):
        """Constructs a module containing the provided aggregateLogic, wired to the
        proper Protobuf entities."""
        protoFilePath = self._writeFilesForRegistration(aggregateName, aggregateLogic)
        self._compileProtobufContracts(protoFilePath, aggregateName)
        self.ImportAggregateLogic('{0}_pb2'.format(aggregateName))
        return self.ImportAggregateLogic(aggregateName)

    def Compile(self, aggregateClassId, aggregateName, aggregateLogic):
        """Handles compilation and persistence of Aggregate logic.
        If the provided aggregateLogic differs from the previously compiled
        logic (or no previously provided logic could be found), the logic
        will be persisted and the version incremented."""
        module = self.BuildModule(aggregateName, aggregateLogic)
        logicMetadata = self._getLogicMetadataWithPersistence(aggregateClassId, aggregateName, aggregateLogic, module)
        return logicMetadata, module

    def _getLogicMetadataWithPersistence(self, aggregateClassId, aggregateName, aggregateLogic, module):
        currentLogicTuple = self.logicStore.GetLatestAggregateLogic(aggregateName)
        if currentLogicTuple is None:
            EventLogger.LogInformationAuto(self, 'Persisting aggregate logic', 'No existing logic found',
                                           tags={'Aggregate': aggregateName})
            # Set logicVersion to 1 since it is the first one
            logicMetadata = self.logicStore.PersistAggregateLogic(aggregateClassId, aggregateName, 1, aggregateLogic, module)
            return logicMetadata
        logicMetadata, currentLogic = currentLogicTuple
        if currentLogic.pythonFileContents != aggregateLogic.pythonFileContents or currentLogic.protoFileContents != aggregateLogic.protoFileContents:
            EventLogger.LogInformationAuto(self, 'Persisting aggregate logic', 'Registered logic and existing logic differ',
                                           tags={'Aggregate': aggregateName})
            logicMetadata = self.logicStore.PersistAggregateLogic(aggregateClassId, aggregateName, logicMetadata.logicVersion + 1, aggregateLogic, module)
            return logicMetadata
        else:
            EventLogger.LogInformationAuto(self, 'Existing aggregate logic found',
                                           tags={'LogicVersion': logicMetadata.logicVersion,
                                                 'LogicId': logicMetadata.logicId})
            return logicMetadata

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
            impfile, pathname, description = imp.find_module(aggregateName)
            return imp.load_module(aggregateName, impfile, pathname, description)
        finally:
            if impfile:
                impfile.close()


class BufferItem(object):
    __metaclass__ = ABCMeta
    def __init__(self):
        self.serializationResult = None

    @abstractmethod
    def Serialize(self):
        pass

SuccessSerializationResult = namedtuple('SuccessSerializationResult', ['serializedResponse', 'eventProto', 'aggregateDict',
                                                                       'aggregate', 'requestId'])
class PersistenceBufferItem(BufferItem):
    def __init__(self, request, aggregate, logicId, singleEvent, receivedTimestamp, processedTimestamp):
        super(PersistenceBufferItem, self).__init__()
        self.request = request
        self.aggregate = aggregate
        self.logicId = logicId
        self.event = singleEvent
        self.receivedTimestamp = receivedTimestamp
        self.processedTimestamp = processedTimestamp

    def Serialize(self):
        eventProto = self.event.ToProto(self.receivedTimestamp, self.processedTimestamp)
        aggregateDict = self.aggregate.ToDict()
        aggregateDict['logicId'] = self.logicId
        response = ChronosResponse(responseCode=ChronosResponse.SUCCESS, requestId=self.request.requestId, #pylint: disable=E1101
                                   senderId=self.request.senderId)
        response.eventProto.CopyFrom(eventProto) #pylint: disable=E1101
        response.aggregateProto.aggregateId = self.aggregate.aggregateId #pylint: disable=E1101
        response.aggregateProto.proto = aggregateDict['proto'] #pylint: disable=E1101
        response.aggregateProto.version = self.aggregate.version #pylint: disable=E1101
        return SuccessSerializationResult(response.SerializeToString(), eventProto, aggregateDict, self.aggregate, self.request.requestId)


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
    def __init__(self, managementNotification):
        super(PersistenceBufferManagementItem, self).__init__()
        self.managementNotification = managementNotification

    def Serialize(self):
        return ManagementSerializationResult(self.managementNotification.SerializeToString())


TagSerializationResult = namedtuple('TagSerializationResult', ['tag', 'aggregateDict'])
class PersistenceBufferTagItem(BufferItem):
    def __init__(self, aggregate, logicId, tag, createDate):
        super(PersistenceBufferTagItem, self).__init__()
        self.aggregate = aggregate
        self.logicId = logicId
        self.tag = tag
        self.createDate = createDate

    def Serialize(self):
        aggregateDict = self.aggregate.ToDict()
        aggregateDict['createDate'] = self.createDate
        aggregateDict['logicId'] = self.logicId
        return TagSerializationResult(self.tag, aggregateDict)


class EventProcessor(object):
    """Handles processing and replay of events against aggregate instances.
    This class is thread-safe."""
    MaxBufferedEvents = 15
    def __init__(self, aggregateClass, eventPersister):
        self.aggregateClass = aggregateClass
        self.eventPersister = eventPersister
        self.persistenceBuffer = []
        self.aggregateSnapshotBuffer = []
        self.aggregateSnapshotTransactionBuffer = []
        self.lock = threading.RLock()
        self.transactionBuffer = None
        self.lostConnection = False

    def _isTransactionInProgress(self):
        return self.transactionBuffer is not None

    def Begin(self):
        if self._isTransactionInProgress():
            return
        try:
            self.CreateTransactionSavepoint()
        except ChronosFailedEventPersistenceException:
            # On the loss of connection, the backend will continually attempt to reconnect and only upon it successfully reconnecting will it return. Therefore,
            # the lostConnection flag below is not intended to indicate the current state of connection just the fact that connection was lost at some point during the
            # process.
            self.lostConnection = True
            self.Begin()
        self.transactionBuffer = []

    def CreateTransactionSavepoint(self):
        self.eventPersister.Begin()

    def Commit(self):
        if not self._isTransactionInProgress():
            return
        for item in self.transactionBuffer:
            serializedItem = item.Serialize()
            self.persistenceBuffer.append(serializedItem)
        try:
            self.transactionBuffer = None
            self.eventPersister.Commit()
            self.aggregateSnapshotBuffer.extend(self.aggregateSnapshotTransactionBuffer)
            self.aggregateSnapshotTransactionBuffer = []
        except ChronosFailedEventPersistenceException:
            self.lostConnection = True

    def Rollback(self):
        self.eventPersister.RollbackNotifier()
        if not self._isTransactionInProgress():
            return
        self.eventPersister.Rollback()
        self.aggregateSnapshotTransactionBuffer = []
        self.transactionBuffer = None

    def Process(self, request, aggregate, logicId, singleEvent):
        """Applies the provided event to the provided aggregate. If the EventProcessor
        is not replaying, the action will be persisted in the event store and all
        subscribed clients will be notified of the event."""
        if aggregate.version != singleEvent.version:
            raise ChronosCoreException('Cannot apply event with mismatching version {0} {1}'.format(aggregate.version, singleEvent.version))

        with self.lock:
            singleEvent.RaiseFor(aggregate)
            aggregate.version += 1
            if not aggregate.IsValid():
                EventLogger.LogWarningAuto(self, 'Aborting event processing', 'Aggregate.IsValid returned false',
                                           tags={'RequestId': request.requestId, 'AggregateId': aggregate.aggregateId})
                raise ChronosCoreException('Aggregate was put into an invalid state')
            bufferItem = PersistenceBufferItem(request, aggregate, logicId, singleEvent, request.receivedTimestamp, long(time.time() * 1e9))
            return aggregate, bufferItem

    def _internalPersist(self, bufferItem):
        if self._isTransactionInProgress():
            self.transactionBuffer.append(bufferItem)
        else:
            self.persistenceBuffer.append(bufferItem.Serialize())

    def TryVerifyEventPersistenceCheckpoint(self):
        self.eventPersister.TryVerifyEventPersistenceCheckpoint()

    def UpsertAggregateSnapshot(self, aggregate):
        try:
            if self.lostConnection:
                self._ensureAggregateSnapshotConsistency()
            self.eventPersister.UpsertAggregateSnapshot(aggregate)
        except ChronosFailedEventPersistenceException:
            self.lostConnection = True
            self.CreateTransactionSavepoint()
            self.UpsertAggregateSnapshot(aggregate)

        if self._isTransactionInProgress():
            self.aggregateSnapshotTransactionBuffer.append(aggregate)
        else:
            self.aggregateSnapshotBuffer.append(aggregate)

    def _ensureAggregateSnapshotConsistency(self):
        for aggregate in self.aggregateSnapshotTransactionBuffer:
            self.eventPersister.UpsertAggregateSnapshot(aggregate)
        for aggregate in self.aggregateSnapshotBuffer:
            self.eventPersister.UpsertAggregateSnapshot(aggregate)
        self.lostConnection = False

    def ProcessIndexDivergence(self, aggregateId):
        """Notifies clients that an aggregate instance has diverged indices."""
        managementNotification = ChronosManagementNotification(notificationType=ChronosManagementNotification.INDEX_DIVERGED, #pylint: disable=E1101
                                                               aggregateId=aggregateId)
        bufferItem = PersistenceBufferManagementItem(managementNotification)
        self._internalPersist(bufferItem)

    def ProcessFailure(self, request, exception):
        """Notifies clients that an event processing request has failed."""
        bufferItem = PersistenceBufferFailureItem(self.aggregateClass, request, exception)
        self._internalPersist(bufferItem)

    def ProcessTag(self, aggregate, logicId, tag, createDate=long(time.time())):
        bufferItem = PersistenceBufferTagItem(aggregate, logicId, tag, createDate)
        self._internalPersist(bufferItem)

    def EnqueueForPersistence(self, bufferItem):
        self._internalPersist(bufferItem)

    def FlushPersistenceBuffer(self, shouldForce=False):
        if self._isTransactionInProgress():
            raise ChronosCoreException('Cannot flush persistence buffer while transaction is in progress')
        with self.lock:
            if shouldForce or len(self.persistenceBuffer) >= self.MaxBufferedEvents:
                self.eventPersister.PersistEvents(self.persistenceBuffer)
                del self.persistenceBuffer[:]
                self.aggregateSnapshotBuffer = []
