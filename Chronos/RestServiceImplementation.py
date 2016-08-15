import cherrypy
import time
from flask import Flask, request, make_response
from paste.translogger import TransLogger
from requests import post

from Chronos.EventLogger import EventLogger
from Chronos.Chronos_pb2 import (ChronosRegistrationRequest, ChronosRequest, ChronosRequestWithTag, ChronosTransactionRequest, ChronosQueryAllRequest,
                                              ChronosQueryByIdRequest, ChronosQueryByIndexRequest, ChronosQueryByTagRequest)

from Chronos.Core import ChronosCoreProvider
from Chronos.Dependency import AggregateSynchronizationManager
from Chronos.DefaultImplementations import GetConfiguredBinding
from Chronos.Infrastructure import AbstractServiceImplementations, AbstractClientProxy, ConfigurablePlugin
from Chronos.Gateway import ChronosGatewayException, ChronosGateway

ISRUNNING = False
EVENTSTORE = None
SYNCHRONIZATIONMANAGER = None
GATEWAY = None
APP = Flask(__name__)

def EnsureRunning():
    if not ISRUNNING:
        raise ChronosGatewayException('Service is not started')

def CreateBinaryResponse(data):
    response = make_response(data)
    response.headers['Content-Type'] = 'application/octet-stream'
    return response

@APP.route('/RegisterAggregate', methods=['POST'])
def RegisterAggregate():
    EnsureRunning()
    chronosRequest = ChronosRegistrationRequest()
    chronosRequest.ParseFromString(request.data)
    return CreateBinaryResponse(GATEWAY.Register(chronosRequest))

@APP.route('/UnregisterAggregate', methods=['POST'])
def UnregisterAggregate():
    EnsureRunning()
    GATEWAY.Unregister(request.data)
    return "OK"

@APP.route('/CheckStatus', methods=['POST'])
def CheckStatus():
    EnsureRunning()
    return CreateBinaryResponse(GATEWAY.CheckStatus(request.data))

@APP.route('/ProcessEvent', methods=['POST'])
def ProcessEvent():
    EnsureRunning()
    chronosRequest = ChronosRequest()
    chronosRequest.ParseFromString(request.data)
    return str(GATEWAY.RouteEvent(chronosRequest))

@APP.route('/ProcessEventWithTag', methods=['POST'])
def ProcessEventWithTag():
    EnsureRunning()
    chronosRequest = ChronosRequestWithTag()
    chronosRequest.ParseFromString(request.data)
    return str(GATEWAY.RouteEventWithTag(chronosRequest))

@APP.route('/ProcessTransaction', methods=['POST'])
def ProcessTransaction():
    EnsureRunning()
    chronosRequest = ChronosTransactionRequest()
    chronosRequest.ParseFromString(request.data)
    return str(GATEWAY.RouteTransaction(chronosRequest))

@APP.route('/GetAll', methods=['POST'])
def GetAll():
    EnsureRunning()
    chronosRequest = ChronosQueryAllRequest()
    chronosRequest.ParseFromString(request.data)
    return CreateBinaryResponse(GATEWAY.GetAll(chronosRequest))

@APP.route('/GetById', methods=['POST'])
def GetById():
    EnsureRunning()
    chronosRequest = ChronosQueryByIdRequest()
    chronosRequest.ParseFromString(request.data)
    return CreateBinaryResponse(GATEWAY.GetById(chronosRequest))

@APP.route('/GetByIndex', methods=['POST'])
def GetByIndex():
    EnsureRunning()
    chronosRequest = ChronosQueryByIndexRequest()
    chronosRequest.ParseFromString(request.data)
    return CreateBinaryResponse(GATEWAY.GetByIndex(chronosRequest))

@APP.route('/GetByTag', methods=['POST'])
def GetByTag():
    EnsureRunning()
    chronosRequest = ChronosQueryByTagRequest()
    chronosRequest.ParseFromString(request.data)
    return CreateBinaryResponse(GATEWAY.GetByTag(chronosRequest))

@APP.route('/GetTags', methods=['POST'])
def GetTags():
    EnsureRunning()
    return CreateBinaryResponse(GATEWAY.GetTags(request.data))


class FlaskLogger(TransLogger):
    """This is a specific logger to use with Flask.
       It redirects Flask logs to our EventLogger for centralized storage.
    """

    def Write_Log(self, environ, method, reqUri, start, status, bytesTag):
        if bytesTag is None:
            bytesTag = '-'
        remoteAddr = '-'
        if environ.get('HTTP_X_FORWARDED_FOR'):
            remoteAddr = environ['HTTP_X_FORWARDED_FOR']
        elif environ.get('REMOTE_ADDR'):
            remoteAddr = environ['REMOTE_ADDR']

        tags = {
            'REMOTE_ADDR': remoteAddr,
            'REMOTE_USER': environ.get('REMOTE_USER') or '-',
            'REQUEST_METHOD': method,
            'REQUEST_URI': reqUri,
            'HTTP_VERSION': environ.get('SERVER_PROTOCOL'),
            'time': time.strftime('%d/%b/%Y:%H:%M:%S', start),
            'status': status.split(None, 1)[0],
            'bytes': bytesTag,
            'HTTP_REFERER': environ.get('HTTP_REFERER', '-'),
            'HTTP_USER_AGENT': environ.get('HTTP_USER_AGENT', '-')
        }

        EventLogger.LogInformation('Service', 'BelvedereTransLogger', 'Write_Log', self.logging_level, tags)

def Exit():
    cherrypy.engine.signal_handler.bus.exit()


class ChronosGatewayRestService(AbstractServiceImplementations):
    def __init__(self, infrastructureProvider):
        AbstractServiceImplementations.__init__(self, infrastructureProvider)
        self.infrastructureProvider = infrastructureProvider
        self.host, self.port = GetConfiguredBinding(self.infrastructureProvider)

    def ProvisionOnStart(self):
        global ISRUNNING  #pylint:disable=W0603
        if ISRUNNING:
            return

        global EVENTSTORE  #pylint:disable=W0603
        global SYNCHRONIZATIONMANAGER  #pylint:disable=W0603
        global GATEWAY  #pylint:disable=W0603

        EVENTSTORE = self.infrastructureProvider.GetConfigurablePlugin(ConfigurablePlugin.EventStore)
        SYNCHRONIZATIONMANAGER = AggregateSynchronizationManager()
        GATEWAY = ChronosGateway(EVENTSTORE, SYNCHRONIZATIONMANAGER, ChronosCoreProvider)
        ISRUNNING = True

    def BlockingRunService(self, commandLineArgs):
        appLogged = FlaskLogger(APP)
        cherrypy.tree.graft(appLogged, '/')
        cherrypy.config.update({
            'engine.autoreload_on': False,
            'log.screen': False,
            'server.socket_port': self.port,
            'server.socket_host': self.host,
            'server.thread_pool': 1
        })
        cherrypy.engine.start()
        cherrypy.engine.signal_handler.handlers['SIGINT'] = Exit
        cherrypy.engine.signal_handler.handlers['SIGTERM'] = Exit
        cherrypy.engine.signal_handler.handlers['SIGUSR1'] = Exit
        cherrypy.engine.signal_handler.subscribe()
        cherrypy.engine.block()
        cherrypy.engine.stop()
        cherrypy.engine.exit()

    def CleanupOnExit(self):
        global ISRUNNING  #pylint:disable=W0603
        if not ISRUNNING:
            return

        global EVENTSTORE  #pylint:disable=W0603
        global GATEWAY  #pylint:disable=W0603

        GATEWAY.Shutdown()
        GATEWAY = None
        EVENTSTORE.Dispose()
        EVENTSTORE = None
        ISRUNNING = False


class ChronosGatewayRestClient(AbstractClientProxy):
    """Rest bindings to the ChronosGateway."""
    def __init__(self, infrastructureProvider):
        AbstractClientProxy.__init__(self, infrastructureProvider)
        host, port = GetConfiguredBinding(infrastructureProvider)
        self.url = 'http://{0}:{1}'.format(host, port)

    def Startup(self, serviceEndpoint):
        pass

    def Shutdown(self):
        pass

    def CallRestService(self, functionName, protoData):
        response = post(url='{0}/{1}'.format(self.url, functionName),
                        data=protoData,
                        headers={'Content-Type': 'application/octet-stream'}).content
        EventLogger.LogInformationAuto(self, 'Response', response, tags={'FunctionName': functionName})
        return response

    def RegisterAggregate(self, chronosRequest):
        return self.CallRestService('RegisterAggregate', chronosRequest)

    def UnregisterAggregate(self, aggregateName):
        return self.CallRestService('UnregisterAggregate', aggregateName)

    def CheckStatus(self, aggregateName):
        return self.CallRestService('CheckStatus', aggregateName)

    def ProcessEvent(self, chronosRequest):
        return long(self.CallRestService('ProcessEvent', chronosRequest))

    def ProcessEventWithTag(self, chronosRequest):
        return long(self.CallRestService('ProcessEventWithTag', chronosRequest))

    def ProcessTransaction(self, chronosRequest):
        return long(self.CallRestService('ProcessTransaction', chronosRequest))

    def GetAll(self, chronosRequest):
        return self.CallRestService('GetAll', chronosRequest)

    def GetById(self, chronosRequest):
        return self.CallRestService('GetById', chronosRequest)

    def GetByIndex(self, chronosRequest):
        return self.CallRestService('GetByIndex', chronosRequest)

    def GetByTag(self, chronosRequest):
        return self.CallRestService('GetByTag', chronosRequest)

    def GetTags(self, aggregateName):
        return self.CallRestService('GetTags', aggregateName)
