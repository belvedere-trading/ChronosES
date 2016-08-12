#pylint: disable=W0703
"""@addtogroup PyInfrastructure
{
"""
import time
from datetime import datetime
from Queue import Queue, Empty
from threading import Lock, Thread

from Chronos.EventLogger import EventLogger

class TimeProvider(object):
    @staticmethod
    def GetTime():
        return datetime.utcnow()

    @staticmethod
    def SecondsSince(past, now):
        if past > now:
            raise ValueError('past is in the future')
        return (now - past).seconds

class BackgroundProcessor(object):
    """Handles processing of parameterless functions/methods on a background thread.
    A single BackgroundProcessor instance can only be started and stopped a single time."""
    def __init__(self, target, triggerPeriod=10, sleepPeriod=.5):
        """@param target the function/method to be invoked in the processing thread
        @param triggerPeriod the timeout (in seconds) specifying how often the target should be called
        @param sleepPeriod the timeout (in seconds) specifying how long the processor should sleep before checking its triggerPeriod
        """
        if not callable(target):
            raise ValueError('target must be callable')
        if sleepPeriod >= triggerPeriod:
            raise ValueError('sleepPeriod must be less than triggerPeriod')
        if sleepPeriod <= 0:
            raise ValueError('sleepPeriod must be greater than zero')
        self.target = target
        self.triggerPeriod = triggerPeriod
        self.sleepPeriod = sleepPeriod
        self.timeProvider = TimeProvider()
        self.thread = Thread(target=self._process)
        self.thread.daemon = True
        self.isRunning = False
        self.lastTriggerTime = None

    def Start(self):
        if self.isRunning:
            raise RuntimeError('already running')
        if self.thread is None:
            raise ValueError('thread disposed')

        self.isRunning = True
        self.lastTriggerTime = self.timeProvider.GetTime()
        self.thread.start()

    def Stop(self):
        if not self.isRunning:
            raise RuntimeError('not running')
        if self.thread is None:
            raise ValueError('thread disposed')
        self.isRunning = False
        self.thread.join()
        self.thread = None

    def _process(self):
        try:
            self.target()
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Error in processing thread')
        while self.isRunning:
            time.sleep(self.sleepPeriod)
            now = self.timeProvider.GetTime()
            if self.timeProvider.SecondsSince(self.lastTriggerTime, now) >= self.triggerPeriod:
                self.lastTriggerTime = now
                try:
                    self.target()
                except Exception:
                    EventLogger.LogExceptionAuto(self, 'Error in processing thread')

class AlreadyProcessingException(Exception):
    pass

class NotProcessingException(Exception):
    pass

class ProcessorDisposedException(Exception):
    pass

class BackgroundQueueProcessor(object):
    """Handles processing of asynchronous tasks in a single-threaded background queue.
    """
    def __init__(self, target, blockingPeriod=.5):
        """Initializes a new processor that will use @p target for processing of queue items.

        @param target A function that will be called for each item enqueue into the processor.
        @param blockingPeriod The length of time (in fractional seconds) that the processor should block while awaiting items to process.
        @throws ValueError if @p target is not callable or @p blockingPeriod is less than .01 seconds.
        """
        if not callable(target):
            raise ValueError('target must be callable')
        if blockingPeriod < .01:
            raise ValueError('blockingPeriod must be >= .01 seconds')
        self.target = target
        self.blockingPeriod = blockingPeriod
        self.queue = Queue()
        self.processingLock = Lock()
        self.isProcessing = False
        self.waitingForCompletion = False
        self.processingThread = Thread(target=self._process)
        self.processingThread.daemon = True

    def StartProcessing(self):
        """Starts the background processing thread and allows items to be enqueued for processing.

        @throws AlreadyProcessingException if the processor instance has already been started.
        @throws ProcessorDisposedException if the processor has been previously shut down.
        """
        with self.processingLock:
            if self.isProcessing:
                raise AlreadyProcessingException()
            if self.processingThread is None:
                raise ProcessorDisposedException()
            self.isProcessing = True
            self.processingThread.start()

    def StopProcessing(self):
        """Immediately stops processing of the background thread.
        After processing is stopped, new items can no longer be added to the processing queue.

        @throws NotProcessingException if the processor has not yet been started.
        @see WaitForCompletion
        """
        with self.processingLock:
            if not self.isProcessing:
                raise NotProcessingException()
            self.isProcessing = False
            self.processingThread = None

    def WaitForCompletion(self, timeout=None):
        """Stops processing, then blocks the caller until all pending background operations have been completed.
        Regardless of success, the process will be shut down upon completion of this method.

        @param timeout the number of seconds to block waiting for processing completion.
        If not supplied, this method will wait indefinitely for completion.
        @returns A bool representing whether or not all items were successfully processed. This can only be False
        if @p timeout is supplied.
        """
        with self.processingLock:
            if not self.isProcessing:
                return True
            self.waitingForCompletion = True
            self.isProcessing = False
            if timeout is not None:
                self.processingThread.join(timeout)
                wasSuccessful = not self.processingThread.is_alive()
            else:
                self.processingThread.join()
                wasSuccessful = True
            self.waitingForCompletion = False
            self.processingThread = None
        return wasSuccessful

    def Enqueue(self, item):
        """Adds an item to the background processing queue.

        @param item the item to be processed. @p target will be called with @p item as its sole parameter.
        @throws NotProcessingException if the processor has not yet been started.
        """
        if not self.isProcessing:
            raise NotProcessingException()
        self.queue.put(item)

    def _process(self):
        while self.isProcessing or self.waitingForCompletion:
            try:
                item = self.queue.get(timeout=self.blockingPeriod)
            except Empty:
                if self.waitingForCompletion:
                    break
                continue
            try:
                self.target(item)
            except Exception:
                EventLogger.LogExceptionAuto(self, 'Background processing of a single item raised an Exception')
