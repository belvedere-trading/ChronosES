import mock
from collections import namedtuple
from unittest import TestCase

class MockEvent(object):
    Dependencies = []
class MockSemanticException(Exception):
    pass

class DependentUponTest(TestCase):
    def setUp(self):
        self.patcher = mock.patch.dict('sys.modules',
        {
            'Chronos.EventLogger': mock.MagicMock(),
            'Chronos.Core': mock.MagicMock(Event=MockEvent, ChronosSemanticException=MockSemanticException)
        })
        self.patcher.start()

        global dependent_upon, ChronosSemanticException, Event
        from Chronos.Core import ChronosSemanticException, Event
        from Chronos.Dependency import dependent_upon

    def tearDown(self):
        MockEvent.Dependencies = [] # Clearing this out so that test cases don't depend on one another
        self.patcher.stop()

    def test_dependent_upon_with_no_source_supplied_should_raise_ChronosSemanticException(self):
        try:
            dependent_upon('name', 'destination')
        except ChronosSemanticException as ex:
            self.assertTrue('must be set' in str(ex), 'Unexpected error message')
        else:
            self.fail('Expected ChronosSemanticException')

    def test_dependent_upon_with_both_sources_supplied_should_raise_ChronosSemanticException(self):
        try:
            dependent_upon('name', 'destination', 'eventSource', 'aggregateSource')
        except ChronosSemanticException as ex:
            self.assertTrue('Only one' in str(ex), 'Unexpected error message')
        else:
            self.fail('Expected ChronosSemanticException')

    def test_dependent_upon_called_on_invalid_class_should_raise_ChronosSemanticException(self):
        try:
            @dependent_upon('name', 'destination', eventSourceProperty='anything')
            class InvalidClass(object):
                pass
        except ChronosSemanticException as ex:
            self.assertTrue('Chronos.Core.Event subclass' in str(ex), 'Unexpected error message')
        else:
            self.fail('Expected ChronosSemanticException')

    def test_dependent_upon_with_invalid_destination_property_should_raise_ChronosSemanticException(self):
        try:
            @dependent_upon('name', 'badPropertyName', eventSourceProperty='anything')
            class TestClass(Event):
                Proto = object
        except ChronosSemanticException as ex:
            self.assertTrue('Invalid destinationProperty' in str(ex), 'Unexpected error message')
        else:
            self.fail('Expected ChronosSemanticException')

    def test_dependent_upon_with_invalid_event_source_property_should_raise_ChronosSemanticException(self):
        try:
            @dependent_upon('name', 'destination', eventSourceProperty='reallyBad')
            class TestClass(Event):
                Proto = namedtuple('Proto', ['destination'])
        except ChronosSemanticException as ex:
            self.assertTrue('Invalid eventSourceProperty' in str(ex), 'Unexpected error message')
        else:
            self.fail('Expected ChronosSemanticException')

    def test_dependent_upon_with_invalid_aggregate_source_property_should_raise_ChronosSemanticException(self):
        try:
            @dependent_upon('name', 'destination', aggregateSourceProperty='lastBadOne')
            class TestClass(Event):
                Proto = namedtuple('Proto', ['destination'])
                Aggregate = mock.MagicMock(Proto=namedtuple('Aggregate', ['someProperty']))
        except ChronosSemanticException as ex:
            self.assertTrue('Invalid aggregateSourceProperty' in str(ex), 'Unexpected error message')
        else:
            self.fail('Expected ChronosSemanticException')

    def test_dependent_upon_with_multiple_dependencies_on_same_aggregate_should_raise_ChronosSemanticException(self):
        try:
            @dependent_upon('duplicate', 'destination', eventSourceProperty='source')
            @dependent_upon('duplicate', 'destination', eventSourceProperty='otherSource')
            class TestClass(Event):
                Aggregate = mock.MagicMock(Dependencies=set())
                Proto = namedtuple('Proto', ['destination', 'source', 'otherSource'])
        except ChronosSemanticException as ex:
            self.assertTrue('Cannot satisfy duplicate' in str(ex))
        else:
            self.fail('Expected ChronosSemanticException')

    def test_dependent_upon_with_valid_destination_and_event_source_property_should_set_dependencies(self):
        @dependent_upon('MyCoolAggregateName', 'greatDestination', eventSourceProperty='awesomeEventSource')
        class TestClass(Event):
            Aggregate = mock.MagicMock(Dependencies=set())
            Proto = namedtuple('Proto', ['greatDestination', 'awesomeEventSource'])

        self.assertTrue('MyCoolAggregateName' in TestClass.Aggregate.Dependencies)
        dependency, = TestClass.Dependencies
        self.assertEqual(dependency.aggregateName, 'MyCoolAggregateName')
        self.assertEqual(dependency.destinationProperty, 'greatDestination')
        self.assertEqual(dependency.eventSourceProperty, 'awesomeEventSource')
        self.assertTrue(dependency.aggregateSourceProperty is None)

    def test_dependent_upon_with_valid_destination_and_aggregate_source_property_should_set_dependencies(self):
        @dependent_upon('MyCoolAggregateName', 'greatDestination', aggregateSourceProperty='finalSource')
        class TestClass(Event):
            Aggregate = mock.MagicMock(Dependencies=set(), Proto=namedtuple('Proto', ['finalSource']))
            Proto = namedtuple('Proto', ['greatDestination'])

        self.assertTrue('MyCoolAggregateName' in TestClass.Aggregate.Dependencies)
        dependency, = TestClass.Dependencies
        self.assertEqual(dependency.aggregateName, 'MyCoolAggregateName')
        self.assertEqual(dependency.destinationProperty, 'greatDestination')
        self.assertEqual(dependency.aggregateSourceProperty, 'finalSource')
        self.assertTrue(dependency.eventSourceProperty is None)

    def test_dependent_upon_with_multiple_dependencies_on_different_aggregates_should_set_dependencies(self):
        @dependent_upon('MyCoolAggregateName', 'greatDestination', aggregateSourceProperty='finalSource')
        @dependent_upon('AnotherAggregate', 'secondDestination', eventSourceProperty='anotherSource')
        class TestClass(Event):
            Aggregate = mock.MagicMock(Dependencies=set(), Proto=namedtuple('Proto', ['finalSource']))
            Proto = namedtuple('Proto', ['greatDestination', 'secondDestination', 'anotherSource'])

        self.assertTrue('MyCoolAggregateName' in TestClass.Aggregate.Dependencies)
        self.assertTrue('AnotherAggregate' in TestClass.Aggregate.Dependencies)
        self.assertEqual(2, len(TestClass.Dependencies))
