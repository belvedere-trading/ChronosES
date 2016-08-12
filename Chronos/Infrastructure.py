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
        The main purpose of this class is to decouple ChronosES from a specific transport implementation.
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
        pass

    @abstractmethod
    def Startup(self, serviceEndpoint):
        pass

    @abstractmethod
    def Shutdown(self):
        pass

    @abstractmethod
    def RegisterAggregate(self, chronosRequest):
        pass

    @abstractmethod
    def UnregisterAggregate(self, aggregateName):
        pass

    @abstractmethod
    def CheckStatus(self, aggregateName):
        pass

    @abstractmethod
    def ProcessEvent(self, chronosRequest):
        pass

    @abstractmethod
    def ProcessEventWithTag(self, chronosRequest):
        pass

    @abstractmethod
    def ProcessTransaction(self, chronosRequest):
        pass

    @abstractmethod
    def GetAll(self, chronosRequest):
        pass

    @abstractmethod
    def GetById(self, chronosRequest):
        pass

    @abstractmethod
    def GetByIndex(self, chronosRequest):
        pass

    @abstractmethod
    def GetByTag(self, chronosRequest):
        pass

    @abstractmethod
    def GetTags(self, aggregateName):
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


class AbstractEventStore(object):
    __metaclass__ = ABCMeta
    def __init__(self, infrastructureProvider):
        """All AbstractEventStore subclass must implement an __init__ that at least satisfies this signature.
        The Chronos::Gateway::ChronosGateway will pass its InfrastructureProvider instance
        into InfrastructureProvider::GetConfiguredEventStore as a means of event store configuration should it become necessary.
        """
        pass

    @abstractproperty
    def KeyGenerator(self): #pylint: disable=C0103
        pass

    @abstractmethod
    def Dispose(self):
        pass

    @abstractmethod
    def GetAggregateId(self, aggregateClass):
        pass

    @abstractmethod
    def GetAndIncrementAggregateId(self, aggregateClass):
        pass

    @abstractmethod
    def PersistAggregateLogic(self, aggregateClass, aggregateLogic):
        pass

    @abstractmethod
    def GetLatestAggregateLogic(self, aggregateClass):
        pass

    @abstractmethod
    def GetLatestAggregateLogicByName(self, aggregateName):
        pass

    @abstractmethod
    def GetAggregateLogic(self, aggregateClass, logicIds):
        pass

    @abstractmethod
    def PersistEvents(self, aggregateClass, events):
        pass

    @abstractmethod
    def PublishManagementNotification(self, aggregateClass, notification):
        pass

    @abstractmethod
    def GetEventPersistenceCheckpoint(self, aggregateClass):
        pass

    @abstractmethod
    def GetEventsToVersion(self, aggregate, toVersion):
        pass

    @abstractmethod
    def GetEventsByTimestampRange(self, aggregate, fromTimestamp, toTimestamp):
        pass

    @abstractmethod
    def TryGetSnapshot(self, aggregateClass, aggregateId):
        pass

    @abstractmethod
    def GetAllSnapshots(self, aggregateClass):
        pass

    @abstractmethod
    def GetIndexedSnapshots(self, aggregateClass, aggregateIds):
        pass

    @abstractmethod
    def GetTags(self, aggregateClass):
        pass

    @abstractmethod
    def GetTag(self, aggregateClass, aggregateId, tag):
        pass

    @abstractmethod
    def GetAllAggregateNames(self):
        pass

    @abstractmethod
    def Subscribe(self, prefix, channel, callback):
        pass

    @abstractmethod
    def Unsubscribe(self, prefix, channel):
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
