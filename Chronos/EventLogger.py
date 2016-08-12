#pylint: disable=W0212, W0702
"""@addtogroup PyInfrastructure
@{
"""
import logging
import os
import errno
import sys
import traceback
from datetime import datetime
import uuid

class EventLoggerException(Exception):
    """Singular exception to be thrown from EventLogger functions"""
    pass

class LoggerLevel(object):
    """Level wrapper to prevent users from having to import logging"""
    Critical = logging.CRITICAL
    Error = logging.ERROR
    Warn = logging.WARN
    Info = logging.INFO
    Debug = logging.DEBUG
    NotSet = logging.NOTSET

class BTFileHandler(logging.FileHandler):
    """FileHandler wrapper to prevent users from having to import logging"""
    def __init__(self, filename, mode='a', encoding=None, delay=False):
        self.CreatePath(os.path.dirname(filename))
        logging.FileHandler.__init__(self, filename, mode, encoding, delay)
        os.chmod(filename, 0777)

    @staticmethod
    def CreatePath(path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if not path or (exc.errno == errno.EEXIST and os.path.isdir(path)):
                pass
            else:
                raise

class BTStreamHandler(logging.StreamHandler):
    """StreamHandler wrapper to prevent users from having to import logging"""
    def __init__(self, stream=None):
        logging.StreamHandler.__init__(self, stream)

class BTNullHandler(logging.Handler):
    """NullHandler wrapper to prevent users from having to import logging"""
    def emit(self, record):
        pass

class PreciseTimeLoggingFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        conv = datetime.utcfromtimestamp(record.created)
        if datefmt:
            return conv.strftime(datefmt)
        else:
            return conv.strftime('%Y-%m-%d %H:%M:%S.%f')

class PersistentLoggerSettings(object):
    """Settings that will be applied to every log written by the EventLogger
    once it's initialized"""
    def __init__(self, level=LoggerLevel.Info, tags=None,
                 logFormat='%(asctime)s UTC; %(message)s',
                 dateFormat=None):
        self.level = level
        if tags is None:
            self.tags = {}
        else:
            self.tags = tags
        self.logFormat = logFormat
        self.dateFormat = dateFormat

    def AddTag(self, key, value):
        self.tags[key] = value

    def PopTag(self, key):
        self.tags.pop(key, None)

    def GetFormatter(self):
        formatter = PreciseTimeLoggingFormatter(self.logFormat, self.dateFormat)
        return formatter


class UnitializedEventLogger(object):
    def _logBasicMessage(self, level, component, module, function, title, reason, tags):
        pass

def GenerateRunId():
    return str(uuid.uuid4())

def GetLogFilePath(firm=None, infoSys=None, whoAmI=None, whereAmI=None, application=None, directory=None):
    if directory is None:
        if os.name == 'posix':
            directory = '/var/btlogs/unprivileged/'
        else:
            directory = 'C:\\var\\log\\BT\\'

    directory = os.path.join(directory, application)
    if not os.path.isdir(directory):
        try:
            os.makedirs(directory)
            os.chmod(directory, 0777)
        except OSError:
            pass

    logFileName = '{0}_{1}_{2}_{3}_{4}.log'.format("All" if firm is None else firm,
                                                   "All" if infoSys is None else infoSys,
                                                   "All" if whoAmI is None else whoAmI,
                                                   "All" if whereAmI is None else whereAmI,
                                                   GenerateRunId())

    logFilePath = os.path.join(directory, logFileName)

    return logFilePath

class EventLogger(object):
    """Handles logging of events that should be consumed by Splunk. A single static EventLogger
    instance should exist for each application, created via one of the initialization
    functions."""
    Initialized = False
    Logger = None

    def __init__(self, application, logFilePath, firm, infoSys, whoAmI, whereAmI,
                 settings, handlers):
        """This constructor should only be called by an initialization function.
        Do not attempt to create your own instance of EventLogger."""
        self.application = application
        self.logFilePath = logFilePath
        self.firm = firm
        self.infoSys = infoSys
        self.whoAmI = whoAmI
        self.whereAmI = whereAmI
        self.settings = settings
        try:
            if handlers is None:
                self.handlers = self._getDefaultHandlers()
            else:
                self.handlers = handlers

            self.logger = logging.getLogger(self.application)
            for handler in self.handlers:
                handler.setFormatter(self.settings.GetFormatter())
                self.logger.addHandler(handler)
            self.SetLevel(self.settings.level)

            # Add a NullHandler to the root logger for propagation
            rootLogger = logging.getLogger()
            if len(rootLogger.handlers) == 0:
                rootLogger.addHandler(BTNullHandler())
        except Exception as ex:
            raise EventLoggerException('Unhandled exception while initializing EventLogger: {0}'.format(ex))

    def _getDefaultHandlers(self):
        return [BTFileHandler(self.logFilePath), BTStreamHandler(sys.stdout)]

    def SetHandlers(self, handlers):
        for handler in self.handlers:
            self.logger.removeHandler(handler)
        self.handlers = handlers
        for handler in self.handlers:
            self.logger.addHandler(handler)

    def ResetHandlers(self):
        self.SetHandlers(self._getDefaultHandlers())

    def SetLevel(self, level):
        self.logger.setLevel(level)

    @staticmethod
    def InitializeLogger(firm=None, infoSys=None, whoAmI=None, whereAmI=None, application=None, directory=None,
                         settings=PersistentLoggerSettings(), handler=None):
        if EventLogger.Initialized:
            return

        if application is None:
            raise ValueError("Application parameter must be set")

        try:
            logFilePath = GetLogFilePath(firm, infoSys, whoAmI, whereAmI, application, directory)

            EventLogger.Logger = EventLogger(application, logFilePath, firm, infoSys, whoAmI, whereAmI,
                                             settings, handler)
            EventLogger.Initialized = True
        except Exception as ex:
            raise EventLoggerException('Unhandled exception while initializing EventLogger: {0}'.format(ex))

    @staticmethod
    def DeinitializeLogger():
        EventLogger.Logger = None
        EventLogger.Initialized = False

    @staticmethod
    def SetGlobalLevel(level):
        if not EventLogger.Initialized:
            return
        EventLogger.Logger.SetLevel(level)

    def _createLogHeader(self, level, component, module, function):
        severity = self._getSeverityForLevel(level)
        severitykvp = self._createSplunkKeyValuePair("Severity", severity)
        componentkvp = self._createSplunkKeyValuePair("Component", component)
        functionkvp = self._createSplunkKeyValuePair("Function", '{0}.{1}'.format(module, function))

        return '{0}{1}{2}'.format(severitykvp, componentkvp, functionkvp)

    @staticmethod
    def _getSeverityForLevel(level):
        if level == LoggerLevel.Critical or level == LoggerLevel.Error:
            return 'Error'
        elif level == LoggerLevel.Warn:
            return 'Warning'
        else:
            return 'Information'

    @staticmethod
    def _createSplunkKeyValuePair(key, value):
        return '{0}={1}; '.format(key, value)

    def _createQuotedSplunkKeyValuePair(self, key, value):
        quotedValue = '"{0}"'.format(value)
        return self._createSplunkKeyValuePair(key, quotedValue)

    def _createBasicMessage(self, level, component, module, function, title, reason):
        header = self._createLogHeader(level, component, module, function)
        titlekvp = self._createQuotedSplunkKeyValuePair("Title", title)
        reasonkvp = '' if reason is None else self._createQuotedSplunkKeyValuePair("Reason", reason)
        message = '{0}{1}{2}'.format(header, titlekvp, reasonkvp)
        return message

    def _logBasicMessage(self, level, component, module, function, title, reason, tags):
        """The logging function for internal EventLogger use. Logs should be written
        by calling one of the static logging functions."""
        message = self._createBasicMessage(level, component, module, function, title, reason)

        message = self._logTags(self.settings.tags, message)
        message = self._logTags(tags, message)

        self._logForLevel(level, message)

    def _logExceptionMessage(self, level, component, module, function, title, reason, tags, exceptionMessage, exceptionStackTrace):
        """The exception logging function for internal EventLogger use. Exception Logs should be written
        by calling one of the static logging functions."""
        message = self._createBasicMessage(level, component, module, function, title, reason)

        message = self._logTags(self.settings.tags, message)
        message += self._createQuotedSplunkKeyValuePair("ExceptionMessage", exceptionMessage)
        message = self._logTags(tags, message)
        message += self._createQuotedSplunkKeyValuePair("ExceptionStackTrace", exceptionStackTrace)

        self._logForLevel(level, message)

    def _logTags(self, tags, message):
        if tags is not None and isinstance(tags, dict):
            for key, value in tags.iteritems():
                if isinstance(value, (int, long, float)):
                    message += self._createSplunkKeyValuePair(key, value)
                else:
                    message += self._createQuotedSplunkKeyValuePair(key, value)
        return message

    def _logForLevel(self, level, message):
        if level == LoggerLevel.Critical:
            self.logger.critical(message)
        elif level == LoggerLevel.Error:
            self.logger.error(message)
        elif level == LoggerLevel.Warn:
            self.logger.warn(message)
        elif level == LoggerLevel.Info:
            self.logger.info(message)
        else:
            self.logger.debug(message)

    @staticmethod
    def GetLogger():
        """Gets the current EventLogger object. This function should be used only
        to set/access properties of the EventLogger instance, not for logging."""
        if not EventLogger.Initialized:
            return UnitializedEventLogger()
        else:
            return EventLogger.Logger

    @staticmethod
    def LogCritical(component, module, function, title, reason=None, tags=None):
        EventLogger.GetLogger()._logBasicMessage(LoggerLevel.Critical, component, module, function, title, reason, tags)

    @staticmethod
    def LogError(component, module, function, title, reason=None, tags=None):
        EventLogger.GetLogger()._logBasicMessage(LoggerLevel.Error, component, module, function, title, reason, tags)

    @staticmethod
    def LogWarning(component, module, function, title, reason=None, tags=None):
        EventLogger.GetLogger()._logBasicMessage(LoggerLevel.Warn, component, module, function, title, reason, tags)

    @staticmethod
    def LogInformation(component, module, function, title, reason=None, tags=None):
        EventLogger.GetLogger()._logBasicMessage(LoggerLevel.Info, component, module, function, title, reason, tags)

    @staticmethod
    def LogDebug(component, module, function, title, reason=None, tags=None):
        EventLogger.GetLogger()._logBasicMessage(LoggerLevel.Debug, component, module, function, title, reason, tags)

    @staticmethod
    def LogException(component, module, function, title=None, reason=None, tags=None):
        try:
            exceptionMessage = traceback.format_exception_only(sys.exc_info()[0], sys.exc_info()[1])[-1].replace('\n', "").replace('"', "'")
            stackTraceWithoutNewlines = [stack.replace('\n', '').replace('"', "'") for stack in traceback.format_tb(sys.exc_info()[2])]
            stackTrace = '\n'.join(stackTraceWithoutNewlines)

            EventLogger.GetLogger()._logBasicMessage(LoggerLevel.Error, component, module, function, title, reason, tags, exceptionMessage, stackTrace)
        except:
            # Raising when we know we're in an except block would be a bad idea
            pass

    @staticmethod
    def LogCriticalAuto(obj, title, reason=None, tags=None):
        function = traceback.extract_stack(None, 2)[0][2]
        EventLogger.GetLogger()._logBasicMessage(LoggerLevel.Critical, obj.__module__, obj.__class__.__name__, function, title, reason, tags)

    @staticmethod
    def LogErrorAuto(obj, title, reason=None, tags=None):
        function = traceback.extract_stack(None, 2)[0][2]
        EventLogger.GetLogger()._logBasicMessage(LoggerLevel.Error, obj.__module__, obj.__class__.__name__, function, title, reason, tags)

    @staticmethod
    def LogWarningAuto(obj, title, reason=None, tags=None):
        function = traceback.extract_stack(None, 2)[0][2]
        EventLogger.GetLogger()._logBasicMessage(LoggerLevel.Warn, obj.__module__, obj.__class__.__name__, function, title, reason, tags)

    @staticmethod
    def LogInformationAuto(obj, title, reason=None, tags=None):
        function = traceback.extract_stack(None, 2)[0][2]
        EventLogger.GetLogger()._logBasicMessage(LoggerLevel.Info, obj.__module__, obj.__class__.__name__, function, title, reason, tags)

    @staticmethod
    def LogDebugAuto(obj, title, reason=None, tags=None):
        function = traceback.extract_stack(None, 2)[0][2]
        EventLogger.GetLogger()._logBasicMessage(LoggerLevel.Debug, obj.__module__, obj.__class__.__name__, function, title, reason, tags)

    @staticmethod
    def LogExceptionAuto(obj, title=None, reason=None, tags=None):
        function = traceback.extract_stack(None, 2)[0][2]

        try:
            exceptionMessage = traceback.format_exception_only(sys.exc_info()[0], sys.exc_info()[1])[-1].replace('\n', "").replace('"', "'")
            stackTraceWithoutNewlines = [stack.replace('\n', '').replace('"', "'") for stack in traceback.format_tb(sys.exc_info()[2])]
            stackTrace = '\n'.join(stackTraceWithoutNewlines)

            EventLogger.GetLogger()._logExceptionMessage(LoggerLevel.Error, obj.__module__, obj.__class__.__name__,
                                                         function, title, reason, tags, exceptionMessage, stackTrace)

        except:
            # Raising when we know we're in an except block would be a bad idea
            pass
