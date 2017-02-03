"""@file
Contains convenience wrappers for profiling the performance of Python code.
@addtogroup Chronos
@{
"""
import cProfile
import os
import pstats
import StringIO
from multiprocessing.managers import BaseManager

class DummyProfiler(object):
    """A fake profiler to be used for no-op instructions when profiling is disabled (ie. in production).
    """
    def __enter__(self):
        return self

    def __exit__(self, excType, excVal, excTb):
        return False

    def Begin(self):
        pass

    def End(self):
        pass


class ProfileAggregator(object):
    """A simple container to handle aggregation of multiple Python profiles into a single view of performance statistics.
    """
    def __init__(self):
        self.aggregatedStats = None

    def AddStats(self, stats):
        """Adds the provided stats to the current tracked statistic.

        @param stats A pstats.Stats instance.
        """
        if self.aggregatedStats is None:
            self.aggregatedStats = stats
        else:
            self.aggregatedStats.add(stats)

    def GetStats(self):
        """Returns all statistics previously aggregated by AddStats.

        @returns A pstats.Stats instance.
        """
        return self.aggregatedStats


class MultiprocessingProfilingContext(object):
    """A profiling context that reports its profiling statistics to an external process for aggregation.
    This class is meant primarily for use within ProcessAwareProfiler; if you need to access functionality from
    this class directly, it would probably be better to create another profiling context implementation than try
    to adapt more functionality into this object.
    """
    def __init__(self, profilingProxy):
        """@param profilingProxy A multiprocessing.BaseProxy instance that should be connected to a remote ProfileAggregator.
        """
        self.profilingProxy = profilingProxy
        self.profile = cProfile.Profile()

    def Begin(self):
        """Starts collecting profiling information.
        """
        self.profile.enable()

    def End(self):
        """Stops collecting profiling information and sends statistics remotely for aggregation.
        It's possible that this method could fail to execute properly if there is an issue with the remote aggregation process;
        in that case, no action will be taken and no Exception will be raised.
        """
        self.profile.disable()
        stats = pstats.Stats(self.profile, stream=StringIO.StringIO())
        try:
            self.profilingProxy.AddStats(stats)
        except Exception:
            pass # Should we log this? Can't throw because profiling shouldn't affect runtime behavior

    def __enter__(self):
        self.Begin()
        return self

    def __exit__(self, excType, excVal, excTb):
        self.End()
        return False


def IsProfilingEnabled():
    return 'BT_PYTHON_ENABLE_PROFILING' in os.environ


class ProcessAwareProfiler(BaseManager):
    """The primary entrypoint into profiling.
    Each application should require at most one instance of ProcessAwareProfiler; if meant to be used with multiprocessing,
    the instance should be made globally available so that it can be used whereever necessary.

    All methods of this class will only have an effect if profiling has been enabled (via setting the BT_PYTHON_ENABLE_PROFILING environment variable).
    """
    def __init__(self, outputFileName):
        """Creates a profiler instance that will output collected statistics to the file specified by @p outputFileName.

        @param outputFileName A file path for profiling output.
        """
        super(ProcessAwareProfiler, self).__init__()
        self.outputFileName = outputFileName
        self.profilingProxy = None

    def Begin(self):
        """Instruments a Python process for future profiling if profiling has been enabled.
        """
        if IsProfilingEnabled() and self.profilingProxy is None:
            self.start()
            self.profilingProxy = self.ProfileAggregator() #pylint: disable=E1101

    def End(self):
        """Outputs collected profiling statistics and disables future profiling.
        """
        if IsProfilingEnabled() and self.profilingProxy is not None:
            self.profilingProxy.GetStats().dump_stats(self.outputFileName)
            self.profilingProxy = None
            self.shutdown()

    def __enter__(self):
        self.Begin()
        return self

    def __exit__(self, excType, excVal, excTb):
        self.End()
        return False

    def GetProfilingInstance(self):
        """Creates a profiling context for collecting performance statistics.
        If profiling has not been enabled, a dummy instance is returned instead.
        """
        if IsProfilingEnabled() and self.profilingProxy is not None:
            return MultiprocessingProfilingContext(profilingProxy=self.profilingProxy)
        else:
            return DummyProfiler()

ProcessAwareProfiler.register('ProfileAggregator', ProfileAggregator)

def Main():
    with profiler.GetProfilingInstance():
        for _ in xrange(10):
            time.sleep(.1)

if __name__ == '__main__':
    import time
    with ProcessAwareProfiler('testing.stat') as profiler:
        Main()
