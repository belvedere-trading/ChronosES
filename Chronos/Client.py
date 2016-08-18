"""@file Logic for Chronos clients.
The public interface for this module is specified fully by ChronosClient.
ChronosClient is an abstract base class that allows for quick and simple implementation of clients for aribitrary Chronos Aggregates.

Detailed documentation and a tutorial can be found here: http://redmine/projects/brown/wiki/Chronos
@addtogroup Chronos
@{
"""
import os
import pkg_resources
import sys
import uuid
from collections import namedtuple
from threading import RLock

from Chronos.EventLogger import EventLogger
from Chronos.Processor import BackgroundQueueProcessor

from Chronos.Chronos_pb2 import (ChronosRegistrationRequest, ChronosRegistrationResponse, ChronosManagementNotification,
                                              ChronosRequest, ChronosRequestWithTag, ChronosTransactionRequest, ChronosResponse, ChronosQueryAllRequest,
                                              ChronosQueryByIdRequest, ChronosQueryByIndexRequest, ChronosQueryResponse,
                                              ChronosQueryByTagRequest, ChronosTagList)

from Chronos.Infrastructure import InfrastructureProvider, ConfigurablePlugin


class ChronosClientException(Exception):
    pass

class SubscriptionManager(object):
    """Handles subscription logic for Chronos Client instances.
    SubscriptionManager is primarily responsible for ensuring that subscriptions remain consistent through Redis
    failures and Chronos index schema divergence.
    """
    def __init__(self, eventStore, aggregateClass, successCallback, failureCallback, managementCallback):
        """Initializes a new SubscriptionManager instance.
        @param eventStore The Chronos::Infrastructure::AbstractEventStore instance to use for subscription
        @param aggregateName the name of the Aggregate to be subscribed for
        @param successCallback a callable object used as the Redis subscription callback for successful Chronos event messages
        @param failureCallback a callable object used as the Redis subscription callback for failed Chronos event messages
        @param managementCallback a callable object used as the Redis subscription callback for Chronos management messages
        """
        self.eventStore = eventStore
        self.aggregateName = aggregateClass.__name__
        self.successCallback = successCallback
        self.failureCallback = failureCallback
        self.managementCallback = managementCallback
        keyGeneratorFunc = eventStore.KeyGenerator
        keyGenerator = keyGeneratorFunc(aggregateClass)
        self.subscriptionPrefix = keyGenerator.hashKey
        self.failureChannel = keyGenerator.GetFailureTopic()
        self.managementChannel = keyGenerator.GetManagementTopic()
        self.subscriptions = {}
        self.lock = RLock() # Needs to be RLock for resubscription
        self.indexedAttributes = None

    def SubscribeForUnifiedUpdates(self):
        """Subscribes @p eventStore for Chronos failure and management messages.
        These messages are considered 'unified' because the subscription channels do not change.
        Additionally, management messages are required to ensure that Aggregate instance subscriptions remain consistent.
        """
        try:
            self.eventStore.Subscribe(self.subscriptionPrefix, self.failureChannel, self.failureCallback)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error in unified subscription: failure channel')
        try:
            self.eventStore.Subscribe(self.subscriptionPrefix, self.managementChannel, self.managementCallback)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error in unified subscription: management channel')
        EventLogger.LogInformationAuto(self, 'Subscribed for unified Chronos updates', tags={'Aggregate': self.aggregateName})

    def UnsubscribeForUnifiedUpdates(self):
        """Unsubscribes for unified updates.
        @see SubscribeForUnifiedUpdates
        """
        try:
            self.eventStore.Unsubscribe(self.subscriptionPrefix, self.failureChannel)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error in unified unsubscription: failure channel')
        try:
            self.eventStore.Unsubscribe(self.subscriptionPrefix, self.managementChannel)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error in unified unsubscription: management channel')
        EventLogger.LogInformationAuto(self, 'Unsubscribed for unified Chronos updates', tags={'Aggregate': self.aggregateName})

    def _getSubscriptionChannel(self, indices):
        subscriptionChannel = self.subscriptionPrefix
        if len(self.indexedAttributes) > 0:
            subscriptionChannel += '.' + '.'.join([str(indices.get(attr, '*')) for attr in self.indexedAttributes])
        return subscriptionChannel

    def Subscribe(self, indices):
        """Subscribes for messages for Aggregate instances that match the fields provided in @p indices.
        @param indices a dict of attribute-value pairs to use for subscription filtering. If an empty dictionary is provided,
        messages from all instances will be received. Otherwise, only instances that match the provided filters will be received.
        It is the client's responsibility to deduplicate any duplicate messages caused by oversubscription!

        @throws ChronosClientException if Resubscribe has not been called prior
        @throws PyDataAccess::Redis::RedisBackend::RedisException if the Redis operation fails.
        The subscription will still eventually be applied if this Exception is raised!
        """
        if self.indexedAttributes is None:
            raise ChronosClientException('Unable to subscribe; indexedAttributes not set')
        subscriptionChannel = self._getSubscriptionChannel(indices)
        with self.lock:
            if subscriptionChannel in self.subscriptions:
                raise ChronosClientException('Already subscribed to {0}'.format(subscriptionChannel))
            self.subscriptions[subscriptionChannel] = indices
            self.eventStore.Subscribe(self.subscriptionPrefix, subscriptionChannel, self.successCallback)
            EventLogger.LogInformationAuto(self, 'Subscribed for Chronos updates', tags={'Aggregate': self.aggregateName,
                                                                                         'Channel': subscriptionChannel})

    def _unsubscribe(self, channel):
        del self.subscriptions[channel]
        self.eventStore.Unsubscribe(self.subscriptionPrefix, channel)
        EventLogger.LogInformationAuto(self, 'Unsubscribed for Chronos updates', tags={'Aggregate': self.aggregateName,
                                                                                       'Channel': channel})

    def Unsubscribe(self, indices):
        """Unsubscribes for messages previously registered by Subscribe.
        @param indices a dict of attribute-value pairs that was previously passed to Subscribe

        @throws ChronosClientException if Resubscribe has not been called prior
        @throws PyDataAccess::Redis::RedisBackend::RedisException if the Redis operation fails.
        The unsubscription will still eventually be applied if this Exception is raised!
        """
        if self.indexedAttributes is None:
            raise ChronosClientException('Unable to unsubscribe; indexedAttributes not set')
        subscriptionChannel = self._getSubscriptionChannel(indices)
        if subscriptionChannel not in self.subscriptions:
            return
        with self.lock:
            self._unsubscribe(subscriptionChannel)

    def UnsubscribeAll(self):
        """Unsubscribes for all updates previously registered by Subscribe.
        """
        if not self.subscriptions:
            return
        EventLogger.LogInformationAuto(self, 'Unsubscribing for all Chronos updates', tags={'Aggregate': self.aggregateName})
        with self.lock:
            for subscription in self.subscriptions.keys():
                try:
                    self._unsubscribe(subscription)
                except Exception:
                    EventLogger.LogExceptionAuto(self, 'Error in unsubscribe all')

    def Resubscribe(self, indexedAttributes):
        """Processes the provided @p indexedAttributes and determines if currently registered subscriptions have to be renewed.
        This method must be called before Subscribe or Unsubscribe.

        @param indexedAttributes a sorted list of attributes that are currently indexed by Chronos for the Aggregate in question
        """
        if indexedAttributes == self.indexedAttributes:
            return
        with self.lock:
            self.indexedAttributes = indexedAttributes
            for channel, indices in self.subscriptions.items():
                try:
                    self._unsubscribe(channel)
                except Exception:
                    EventLogger.LogExceptionAuto(self, 'Error in resubscription: unsubscribing')
                try:
                    self.Subscribe(indices)
                except Exception:
                    EventLogger.LogExceptionAuto(self, 'Error in resubscription: subscribing')

class TagNotFound(Exception):
    """Raised when a requested tag was not known by the Chronos service.
    This Exception exists to differentiate service failures from missing tags."""
    pass

AggregateResponse = namedtuple('AggregateResponse', ['aggregateId', 'version', 'aggregate'])

EventResponse = namedtuple('EventResponse', ['version', 'logicVersion', 'event', 'receivedTimestamp', 'processedTimestamp'])

class ChronosTransactionItem(object):
    """Represents a single action that should take place as part of an atomic Chronos transaction.
    """
    def __init__(self, event, aggregateId=0, tag=None, tagExpiration=0):
        self.event = event
        self.aggregateId = aggregateId
        self.tag = tag
        self.tagExpiration = tagExpiration

class ChronosClient(object):
    # Allows for simpler parsing in ParseEventResponse
    EventTypes = ()
    ServiceName = 'Chronos'
    def __init__(self, aggregateProtoClass, callback=None, errorCallback=None,
                 managementCallback=None, infrastructureProvider=None, **kwargs):
        if infrastructureProvider is None:
            infrastructureProvider = InfrastructureProvider()
        self.clientId = uuid.uuid4()
        self.aggregateProtoClass = aggregateProtoClass
        self.aggregateName = aggregateProtoClass.__name__
        self.callback = callback
        self.errorCallback = errorCallback
        self.managementCallback = managementCallback
        self.successProcessor = BackgroundQueueProcessor(target=self._successCallback)
        self.successProcessor.StartProcessing()
        self.failureProcessor = BackgroundQueueProcessor(target=self._failureCallback)
        self.failureProcessor.StartProcessing()
        self.managementProcessor = BackgroundQueueProcessor(target=self._managementCallback)
        self.managementProcessor.StartProcessing()
        self.subscriptionManager = SubscriptionManager(infrastructureProvider.GetConfigurablePlugin(ConfigurablePlugin.EventStore, **kwargs),
                                                       self.aggregateProtoClass,
                                                       self._internalHandleNotification,
                                                       self._internalHandleNotification,
                                                       self.managementProcessor.Enqueue)
        self.serviceProxyManager = infrastructureProvider.GetConfigurablePlugin(ConfigurablePlugin.ServiceProxyManager, **kwargs)
        self.subscriptionManager.SubscribeForUnifiedUpdates()
        try:
            self.client = self.serviceProxyManager.Connect()
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Failed to connect to Chronos service')
            raise

    def Connect(self):
        self.client = self.serviceProxyManager.Connect()

    def Disconnect(self):
        self.serviceProxyManager.Disconnect()
        self.client = None

    def Dispose(self):
        self.subscriptionManager.UnsubscribeForUnifiedUpdates()
        self.subscriptionManager.UnsubscribeAll()
        self.successProcessor.StopProcessing()
        self.failureProcessor.StopProcessing()
        self.managementProcessor.StopProcessing()
        if self.serviceProxyManager is not None:
            try:
                self.serviceProxyManager.Dispose()
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Failed to dispose consumer')
            finally:
                self.serviceProxyManager = None

    def TypeOf(self, eventClass):
        """Returns the stringified type of the provided event class.
        This string should be used with EventProto.type to determine if the type
        of a given event matches a known type."""
        return '{0}.{1}'.format(self.aggregateName, eventClass.__name__)

    def ParseAggregate(self, aggregateProto):
        """Parses the provided AggregateProto instance into an instance of AggregateResponse
        for easier client-side use."""
        aggregate = self.aggregateProtoClass()
        aggregate.ParseFromString(aggregateProto.proto)

        return AggregateResponse(aggregateProto.aggregateId, aggregateProto.version, aggregate)

    @staticmethod
    def ParseEvent(eventClass, eventProto):
        """Parses the provided EventProto instance into an instance of the provided event protobuf entity.
        This method should be used when the type of an event is known prior to parsing, and only the
        information in the protobuf entity is required (that is, no Chronos metadata is required)."""
        event = eventClass()
        event.ParseFromString(eventProto.proto)

        return event

    def ParseEventResponse(self, eventProto):
        """Parses the provided EventProto instance into an instance of EventResponse for easier client-side use.
        This method uses the EventTypes class member to find eligible event types for parsing. If a matching
        event type is not found, ValueError will be raised."""
        for eventType in self.EventTypes:
            if eventProto.type == self.TypeOf(eventType):
                event = eventType()
                break
        else:
            raise ValueError('Unknown event type {0}'.format(eventProto.type))

        event = eventType() #pylint: disable=W0631
        event.ParseFromString(eventProto.proto)
        return EventResponse(eventProto.version, eventProto.logicVersion, event, eventProto.receivedTimestamp,
                             eventProto.processedTimestamp)

    def _processRawRegistrationReponse(self, resp):
        response = ChronosRegistrationResponse()
        response.ParseFromString(resp)
        if response.responseCode == ChronosRegistrationResponse.SUCCESS: #pylint: disable=E1101
            self.subscriptionManager.Resubscribe(response.indexedAttributes) #pylint: disable=E1101
        return response

    def Register(self):
        EventLogger.LogInformationAuto(self, 'Registering aggregate',
                                       tags={'Aggregate': self.aggregateName})
        try:
            request = ChronosRegistrationRequest()
            request.aggregateName = self.aggregateName

            chronosScriptLocation = os.getenv('CHRONOS_SCRIPT_LOCATION', None)
            if chronosScriptLocation is not None:
                with open(os.path.join(chronosScriptLocation, self.aggregateName + '.py'), 'r') as pythonFile:
                    pythonFileContents = pythonFile.read()
                with open(os.path.join(chronosScriptLocation, self.aggregateName + '.proto'), 'r') as protoFile:
                    protoFileContents = protoFile.read()
            else:
                moduleName = self.__module__.split('.', 1)[0]
                pythonFileContents = pkg_resources.resource_string(moduleName,
                                                                   os.path.join('ChronosScripts', self.aggregateName + '.py'))
                protoFileContents = pkg_resources.resource_string(moduleName,
                                                                  os.path.join('ChronosScripts', self.aggregateName + '.proto'))
            request.aggregateLogic.pythonFileContents = pythonFileContents #pylint: disable=E1101
            request.aggregateLogic.protoFileContents = protoFileContents #pylint: disable=E1101
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error building registration request',
                                         tags={'Aggregate': self.aggregateName})
            excInfo = sys.exc_info()
            raise ChronosClientException, excInfo[1], excInfo[2]

        try:
            resp = self.client.RegisterAggregate(request.SerializeToString())
            return self._processRawRegistrationReponse(resp)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error registering', tags={'Aggregate': self.aggregateName})
            excInfo = sys.exc_info()
            raise ChronosClientException, excInfo[1], excInfo[2]

    def CheckStatus(self):
        """Retrieves the Registration status of the client's Aggregate from Chronos.
        Either this method or Register must be called in order to use Subscribe.
        """
        try:
            resp = self.client.CheckStatus(self.aggregateName)
            return self._processRawRegistrationReponse(resp)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error checking status',
                                         tags={'Aggregate': self.aggregateName})
            excInfo = sys.exc_info()
            raise ChronosClientException, excInfo[1], excInfo[2]

    def Unregister(self):
        self.client.UnregisterAggregate(self.aggregateName)

    def RaiseEvent(self, event, aggregateId=0):
        try:
            request = ChronosRequest(aggregateId=aggregateId, senderId=self.clientId.bytes,
                                     eventType=self.TypeOf(event.__class__),
                                     eventProto=event.SerializeToString())
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error building event request',
                                         tags={'Aggregate': self.aggregateName})
            excInfo = sys.exc_info()
            raise ChronosClientException, excInfo[1], excInfo[2]

        requestId = self.client.ProcessEvent(request.SerializeToString())
        return requestId

    def RaiseEventWithTag(self, tag, event, aggregateId=0, tagExpiration=0):
        try:
            request = ChronosRequestWithTag(tag=tag, tagExpiration=tagExpiration)
            request.request.senderId = self.clientId.bytes #pylint: disable=E1101
            request.request.aggregateId = aggregateId #pylint: disable=E1101
            request.request.eventType = self.TypeOf(event.__class__) #pylint: disable=E1101
            request.request.eventProto = event.SerializeToString() #pylint: disable=E1101
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error building event request',
                                         tags={'Aggregate': self.aggregateName})
            excInfo = sys.exc_info()
            raise ChronosClientException, excInfo[1], excInfo[2]

        requestId = self.client.ProcessEventWithTag(request.SerializeToString())
        return requestId

    def RaiseTransaction(self, events):
        request = ChronosTransactionRequest()
        try:
            for event in events:
                item = request.requests.add() # pylint: disable=E1101
                if event.tag is not None:
                    request.tag = event.tag
                    request.tagExpiration = event.tagExpiration
                item.request.senderId = self.clientId.bytes #pylint: disable=E1101
                item.request.aggregateId = event.aggregateId #pylint: disable=E1101
                item.request.eventType = self.TypeOf(event.event.__class__) #pylint: disable=E1101
                item.request.eventProto = event.event.SerializeToString() #pylint: disable=E1101
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error building transaction request',
                                         tags={'Aggregate': self.aggregateName})
            excInfo = sys.exc_info()
            raise ChronosClientException, excInfo[1], excInfo[2]

        requestId = self.client.ProcessTransaction(request.SerializeToString())
        return requestId

    def _internalHandleNotification(self, message):
        try:
            data = message['data']
            response = ChronosResponse()
            response.ParseFromString(data)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error parsing Redis message')
            return
        sentByMe = uuid.UUID(bytes=response.senderId) == self.clientId #pylint: disable=E1101
        if response.responseCode == ChronosResponse.SUCCESS: #pylint: disable=E1101
            self.successProcessor.Enqueue((response, sentByMe))
        else:
            self.failureProcessor.Enqueue((response, sentByMe))

    def _successCallback(self, successItem):
        if self.callback is not None:
            chronosResponse, wasSentByMe = successItem
            try:
                aggregateResponse = self.ParseAggregate(chronosResponse.aggregateProto)
                eventResponse = self.ParseEventResponse(chronosResponse.eventProto)
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Error parsing Chronos response')
                return
            self.callback(chronosResponse.requestId, aggregateResponse, eventResponse, wasSentByMe)

    def _failureCallback(self, failureItem):
        if self.errorCallback is not None:
            chronosResponse, wasSentByMe = failureItem
            self.errorCallback(chronosResponse, wasSentByMe)

    def _managementCallback(self, message):
        try:
            data = message['data']
            notification = ChronosManagementNotification()
            notification.ParseFromString(data)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error parsing Redis message')
            return
        if notification.notificationType == ChronosManagementNotification.REGISTERED: #pylint: disable=E1101
            self.subscriptionManager.Resubscribe(notification.indexedAttributes) #pylint: disable=E1101
        if self.managementCallback is not None:
            self.managementCallback(notification)

    def Subscribe(self, **indices):
        try:
            self.subscriptionManager.Subscribe(indices)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error subscribing for Chronos updates', tags=indices)

    def Unsubscribe(self, **indices):
        try:
            self.subscriptionManager.Unsubscribe(indices)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error unsubscribing for Chronos updates', tags=indices)

    def UnsubscribeAll(self):
        try:
            self.subscriptionManager.UnsubscribeAll()
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error unsubscribing for Chronos updates')

    def GetAllAggregates(self):
        try:
            request = ChronosQueryAllRequest()
            request.aggregateName = self.aggregateName
            serverResponse = self.client.GetAll(request.SerializeToString())
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Unable to retrieve aggregates')
            excInfo = sys.exc_info()
            raise ChronosClientException, excInfo[1], excInfo[2]

        return self._getQueryResponse(serverResponse)

    def GetAggregateById(self, aggregateId):
        try:
            request = ChronosQueryByIdRequest()
            request.aggregateName = self.aggregateName
            request.aggregateId = aggregateId
            serverResponse = self.client.GetById(request.SerializeToString())
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Unable to retrieve aggregate',
                                         tags={'AggregateId': aggregateId})
            excInfo = sys.exc_info()
            raise ChronosClientException, excInfo[1], excInfo[2]

        return self._getQueryResponse(serverResponse)[0]

    def GetAggregatesByIndex(self, **kwargs):
        try:
            request = ChronosQueryByIndexRequest()
            request.aggregateName = self.aggregateName
            for key, value in kwargs.iteritems():
                request.indexKeys.append(key) #pylint: disable=E1101
                request.indexKeys.append(str(value)) #pylint: disable=E1101
                serverResponse = self.client.GetByIndex(request.SerializeToString())
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Unable to retrieve aggregates from index',
                                         tags=kwargs)
            excInfo = sys.exc_info()
            raise ChronosClientException, excInfo[1], excInfo[2]

        return self._getQueryResponse(serverResponse)

    def _getQueryResponse(self, serverResponse):
        try:
            response = ChronosQueryResponse()
            response.ParseFromString(serverResponse)
            return [self.ParseAggregate(aggregate) for aggregate in response.aggregates] #pylint: disable=E1101
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error parsing query response',
                                         tags={'Aggregate': self.aggregateName})
            excInfo = sys.exc_info()
            raise ChronosClientException, excInfo[1], excInfo[2]

    def GetAggregateByTag(self, aggregateId, tag):
        try:
            request = ChronosQueryByTagRequest()
            request.aggregateName = self.aggregateName
            request.tag = tag
            request.aggregateId = aggregateId
            serverResponse = self.client.GetByTag(request.SerializeToString())
            response = ChronosQueryResponse()
            response.ParseFromString(serverResponse)
            if response.responseCode == ChronosQueryResponse.TAG_NOT_FOUND: #pylint: disable=E1101
                raise TagNotFound('Unknown tag {0}'.format(tag))

            return self.ParseAggregate(response.aggregates[0])  #pylint: disable=E1101
        except TagNotFound:
            raise
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error retrieving tag', tags={'Tag': tag})
            excInfo = sys.exc_info()
            raise ChronosClientException, excInfo[1], excInfo[2]

    def GetAllTags(self):
        try:
            response = ChronosTagList()
            response.ParseFromString(self.client.GetTags(self.aggregateName))
            return [tag for tag in response.tags] #pylint: disable=E1101
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error retrieving all tags')
            excInfo = sys.exc_info()
            raise ChronosClientException, excInfo[1], excInfo[2]
