"""@file
Contains data structures for more advanced mapping functionality.
@addtogroup PyInfrastructure
@{
"""
from collections import defaultdict
from ordereddict import OrderedDict
from UserDict import DictMixin

class MultimapValueMeta(type):
    """A simple metaclass that tags indexing functions on MultimapValue instances for use by ConjoiningIndexedMultimap.
    This class should not be used directly by clients; ConjoiningIndexedMultimap value types should subclass
    MultimapValue instead."""
    def __new__(mcs, name, bases, namespace, **_):
        #pylint: disable=C0103,W0212
        result = type.__new__(mcs, name, bases, dict(namespace))
        result._indices = dict((func.__name__, func) for func in namespace.values() if hasattr(func, '_multimapIndex'))
        return result

class MultimapValue(object):
    """A thin base class that uses the MultimapValueMeta metaclass."""
    __metaclass__ = MultimapValueMeta

def index(func): #pylint: disable=C0103
    """A method decorator that tags the associated method for indexing by ConjoiningIndexedMultimap.
    This decorator should only be used in subclasses of MultimapValue."""
    func._multimapIndex = True #pylint: disable=W0212
    return func

class ConjoiningIndexedMultimap(dict):
    """A map that allows its values to be accessed by multiple (potentially shared) keys.

    Values must be instances of the supplied valueType argument, which in turn must be a subclass
    of MultimapValue. In addition, the supplied value type must specify at least one index
    (through use of the index decorator).

    ConjoiningIndexedMultimap requires that each inserted value has a unique key to identify it.
    As values are added to the map using the unique key, additional keys are generated using
    user-defined selectors. All values that shared a generated key will be returned when that
    generated key is accessed.

    When a value is removed from the map (once again, using its unique key), it will also be
    removed from all other mappings.

    Caveats:

    Values inserted into ConjoiningIndexedMultimap should not override __eq__ or __hash__ to ensure that
    the generated lookup sets compare values based on their identities and not their values.

    The same value should never be inserted into ConjoiningIndexedMultimap more than once; doing so will
    result in the eventual orphaning of that object in all generated lookup sets once that value
    is removed based on any one of its keys.

    If any of the generated keys return a simple iterable value (a value that has an __iter__ method),
    keys will be generated for each item within the iterable, rather than using the iterable itself.
    This behavior exists to allow for indexing on functions that return multiple values.

    @see index
    @see MultimapValue

    For example:
    @code
    from Chronos.Map import ConjoiningIndexedMultimap, MultimapValue, index

    class TestUser(MultimapValue):
        def __init__(self, userId, whoAmI, whereAmI):
            self.userId = userId
            self.whoAmI = whoAmI
            self.whereAmI = whereAmI

        @index
        def WhoAmI(self):
            return self.whoAmI

        @index
        def WhereAmI(self):
            return self.whereAmI

    conMap = ConjoiningIndexedMultimap(valueType=TestUser)
    u1 = TestUser(1, 'jka', 'testclerk1')
    u2 = TestUser(2, 'jka', 'readonly1')
    u3 = TestUser(3, 'jme', 'testclerk1')
    conMap[1] = u1
    conMap[2] = u2
    conMap[3] = u3

    conMap.WhoAmI('jme') == set([u3])
    conMap.WhereAmI('testclerk1') == set([u1, u3])
    conMap.WhoAmI('jka') == set([u1, u2])
    @endcode"""
    def __init__(self, valueType):
        """Creates an instance of ConjoiningIndexedMultimap using the supplied value type to construct generated indices."""
        #pylint: disable=W0212
        if not issubclass(valueType, MultimapValue):
            raise ValueError('valueType must be a subclass of MultimapValue')
        if not valueType._indices:
            raise ValueError('valueType must specify at least one index')
        super(ConjoiningIndexedMultimap, self).__init__()
        self.valueType = valueType
        self.generationFunctions = {}
        self.indexedViews = {}
        for functionName, function in valueType._indices.iteritems():
            if functionName in self.__dict__:
                raise ValueError('invalid function name, namespace already contains {0}'.format(functionName))
            self.generationFunctions[functionName] = function
            self.indexedViews[functionName] = defaultdict(set)
            def func(indexKey, functionName=functionName): #pylint: disable=C0103
                return self.indexedViews[functionName][indexKey]
            setattr(self, functionName, func)

    def __setitem__(self, key, value):
        if value is None:
            raise ValueError('value cannot be None')
        if not isinstance(value, self.valueType):
            raise ValueError('value must be an instance of valueType')

        del self[key]
        for functionName, function in self.generationFunctions.iteritems():
            indexKey = function(value)
            if hasattr(indexKey, '__iter__'):
                for val in indexKey:
                    self.indexedViews[functionName][val].add(value)
            else:
                self.indexedViews[functionName][indexKey].add(value)
        super(ConjoiningIndexedMultimap, self).__setitem__(key, value)

    def __delitem__(self, key):
        value = self.pop(key, None)
        if value is None:
            return

        for functionName, function in self.generationFunctions.iteritems():
            indexKey = function(value)
            if hasattr(indexKey, '__iter__'):
                for val in indexKey:
                    self.indexedViews[functionName][val].discard(value)
            else:
                self.indexedViews[functionName][indexKey].discard(value)

    def GetIntersection(self, **kwargs):
        """Returns the intersection of all lookups specified in the keyword arguments.
        This method can accept any keyword argument matching the name of one of the indexed
        lookup functions on a ConjoiningIndexedMultimap instance. The returned value will always
        be a set containing the values that satisfied all provided lookups. If no arguments
        are provided, a set containing all values will be returned instead.

        Continuing from the previous example code:

        @code
        conMap.GetIntersection(WhereAmI='testclerk1') == set([u1, u3])
        conMap.GetIntersection(WhoAmI='jka', WhereAmI='testclerk1') == set([u1])
        @endcode"""
        if not kwargs:
            return set(self.values())

        values = []
        for lookupName, lookupValue in kwargs.iteritems():
            lookupFunc = getattr(self, lookupName, None)
            if not callable(lookupFunc):
                raise ValueError('Lookup function {0} not found'.format(lookupName))
            values.append(lookupFunc(lookupValue))

        if not values:
            return set()
        elif len(values) == 1:
            return values[0]
        else:
            return values[0].intersection(*values[1:])

class LRUEvictingMap(DictMixin):
    """A dictionary that maintains a predefined maximum capacity.
    Upon reaching capacity, LRUEvictingMap will begin replacing the least recently used (LRU) item
    when new items are added to the dictionary.
    This container is NOT threadsafe.
    """
    def __init__(self, capacity=1000):
        """Create an instance of the dictionary with the specified maximum capacity.

        @param capacity An int specifying the maximum number of items that should be kept in the dictionary.
        @throws ValueError if @p capacity is not a natural number
        """
        if not isinstance(capacity, int) or capacity <= 0:
            raise ValueError('capacity must be a natural number')
        self.capacity = capacity
        self.cache = OrderedDict()

    def __setitem__(self, key, value):
        try:
            del self.cache[key]
        except KeyError:
            if len(self) >= self.capacity:
                self.cache.popitem(last=False)
        self.cache[key] = value

    def __getitem__(self, key):
        value = self.cache.pop(key)
        self.cache[key] = value
        return value

    def __delitem__(self, key):
        del self.cache[key]

    def keys(self): #pylint: disable=C0103
        return self.cache.keys()
