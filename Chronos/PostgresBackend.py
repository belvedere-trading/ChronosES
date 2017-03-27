"""@addtogroup Chronos
@{
"""
import functools
import time
import psycopg2
from psycopg2.extras import Json

from Chronos.EventLogger import EventLogger
from Chronos.Core import ChronosFailedEventPersistenceException

DEFAULTINSTANCESTEP = 5000000
DEFAULTEVENTSTEP = 5000000

class PostgresStatement(object):
    GetAggregateMetadata = """
    SELECT chronos_aggregate_class_id
    FROM chronos_aggregate
    WHERE aggregate_name=%s;"""

    InsertAggregateMetadata = """
    INSERT INTO chronos_aggregate(aggregate_name, current_instance_partition_boundary, current_event_partition_boundary)
    VALUES(%s, %s, %s);"""

    GetAggregateEventAndInstancePartitionBoundaries = """
    SELECT current_event_partition_boundary, current_instance_partition_boundary
    FROM chronos_aggregate
    WHERE aggregate_name=%s;"""

    GetAggregateId = """
    SELECT chronos_aggregate_id
    FROM {}
    ORDER BY chronos_aggregate_id DESC
    LIMIT 1;"""

    GetSnapshot = """
    WITH event AS (
        SELECT chronos_aggregate_id, MAX(event_version) as version
        FROM {}
        GROUP BY chronos_aggregate_id
    )
    SELECT instance.chronos_aggregate_id, instance.snapshot, event.version
    FROM {} AS instance
    INNER JOIN event ON instance.chronos_aggregate_id = event.chronos_aggregate_id"""

    GetCount = """
    SELECT COUNT(*) FROM {};
    """

    LimitAndOffset = """
    LIMIT %s OFFSET %s"""

    OrderBy = """
    ORDER BY instance.index_values->>%s"""

    OrderByAggregateId = """
    ORDER BY chronos_aggregate_id"""

    WhereSingleSnapshot = """
    WHERE instance.chronos_aggregate_id=%s;"""

    GetTag = """
    SELECT chronos_aggregate_id, tag_proto, tag_version
    FROM {}
    WHERE chronos_aggregate_id=%s AND tag=%s;"""

    GetTagNames = """
    SELECT tag
    FROM {}
    WHERE chronos_aggregate_id=%s;"""

    # NOTE: We can't leverage the postgres ON CONFLICT DO UPDATE because it doesn't work with table inheritance
    UpsertAggregateInstance = """
    SELECT upsert_{}(%s::bigint, %s::json, %s);"""

    InsertAggregateLogic = """
    INSERT INTO chronos_logic(chronos_aggregate_class_id, logic_version, python_logic, proto_contract, constraints)
    VALUES(%s, %s, %s, %s, %s);
    SELECT LASTVAL();"""

    GetAggregateLogic = """
    SELECT chronos_logic_id, logic_version, python_logic, proto_contract, constraints
    FROM chronos_logic
    INNER JOIN chronos_aggregate ON chronos_logic.chronos_aggregate_class_id = chronos_aggregate.chronos_aggregate_class_id
    WHERE aggregate_name=%s"""

    GetLatestConstraints = """
    SELECT constraints
    FROM chronos_logic
    INNER JOIN chronos_aggregate ON chronos_logic.chronos_aggregate_class_id = chronos_aggregate.chronos_aggregate_class_id
    WHERE aggregate_name=%s
    ORDER BY logic_version DESC
    LIMIT 1;"""

    OrderByLatestAggregateLogic = """
    ORDER BY logic_version DESC
    LIMIT 1;"""

    AndMatchesPreviousAggregateLogic = """
    AND logic_version = ANY(%s);"""

    GetAllAggregateNames = """
    SELECT aggregate_name
    FROM chronos_aggregate;"""

    # ON CONFLICT ... DO NOTHING does not return, so this "updates" by setting the conflicting field to the same value so we can return the ID
    InsertOrGetEventType = """
    INSERT INTO {}(event_type)
    VALUES(%s)
    ON CONFLICT (event_type) DO UPDATE SET event_type=EXCLUDED.event_type
    RETURNING chronos_event_type_id;"""

    InsertEvent = """
    INSERT INTO {}(chronos_aggregate_id, chronos_logic_id, chronos_event_type_id, event_version, event_proto, received_timestamp, processed_timestamp)
    VALUES(%s, %s, %s, %s, %s, %s, %s);
    SELECT LASTVAL();"""

    InsertTag = """
    INSERT INTO {}(chronos_aggregate_id, chronos_logic_id, tag, tag_proto, tag_version, tag_timestamp)
    VALUES(%s, %s, %s, %s, %s, %s);"""

    GetLatestEventId = """
    SELECT chronos_event_id
    FROM {}
    ORDER BY chronos_event_id DESC
    LIMIT 1;"""

    GetEventsBase = """
    SELECT event_type, event_version, logic_version, event_proto, received_timestamp, processed_timestamp
    FROM {} AS eventBase
    INNER JOIN {} AS eventType ON eventbase.chronos_event_type_id = eventType.chronos_event_type_id
    INNER JOIN chronos_logic AS logic ON eventBase.chronos_logic_id = logic.chronos_logic_id"""

    GetEventsToVersion = """
    WHERE chronos_aggregate_id=%s AND event_version BETWEEN %s AND %s
    ORDER BY event_version;"""

    GetEventsByTimestamp = """
    WHERE chronos_aggregate_id=%s AND received_timestamp BETWEEN %s AND %s
    ORDER BY received_timestamp;"""

    GetStagedEvents = """
    SELECT request_id, index_values, serialized_response
    FROM {}
    ORDER BY event_persistence_id DESC;"""

    InsertCheckpoint = """
    INSERT INTO {} (request_id, index_values, serialized_response)
    VALUES (%s, %s, %s);"""

    CreateIndex = """
    CREATE {}INDEX {}
    ON {}_{}_{} """

    CreateCheckConstraint = """
    ALTER TABLE {}_{}_{} ADD CONSTRAINT {} CHECK ({})"""

    DropIndex = """
    DROP INDEX {};"""

    DropConstraint = """
    ALTER TABLE {}_{}_{} DROP CONSTRAINT {};"""

    DeleteAllRows = """
    DELETE FROM {};"""

# This decorator is to be used when simply retrying the function after a disconnect could lose data
# NOTE: This decorator must only be used on instance methods (have 'self' as the first argument)
def stateful_database_operation(function): #pylint: disable=C0103
    @functools.wraps(function)
    def Wrapper(self, *args, **kwargs):
        try:
            return function(self, *args, **kwargs)
        except psycopg2.OperationalError:
            EventLogger.LogExceptionAuto(self, 'Attempting to reconnect to database', 'Connection to database likely lost')
            self.Connect()
            if hasattr(self, 'reader'):
                self.reader.Connect()
            raise ChronosFailedEventPersistenceException('Lost connection to Postgres')
    return Wrapper

# This decorator is to be used for operations that can safely be retried after a disconnect
# NOTE: This decorator must only be used on instance methods (have 'self' as the first argument)
def stateless_database_operation(function): #pylint: disable=C0103
    @functools.wraps(function)
    def Wrapper(self, *args, **kwargs):
        while self.tryReconnect:
            try:
                return function(self, *args, **kwargs)
            except psycopg2.OperationalError:
                EventLogger.LogExceptionAuto(self, 'Attempting to reconnect to database', 'Connection to database likely lost')
                self.Connect()
                if hasattr(self, 'reader'):
                    self.reader.Connect()
    return Wrapper

class PostgresConnectionBase(object):
    def __init__(self, user, password, database, host, port):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.connection = None
        self.tryReconnect = True
        self.Connect()

    def Connect(self):
        while self.tryReconnect:
            try:
                self.connection = psycopg2.connect(user=self.user, password=self.password, database=self.database, host=self.host, port=self.port)
            except psycopg2.OperationalError:
                EventLogger.LogExceptionAuto(self, 'Attempting to reconnect to database', 'psycopg2.connect() failed', tags={'User':self.user, 'Database':self.database,
                                                                                                                             'Host':self.host, 'Port':self.port})
                time.sleep(1)
            else:
                break

    def Dispose(self):
        self.tryReconnect = False
        self.connection.close()

    def ReturnOne(self, query, *args, **kwargs):
        result = self.ReturnAll(query, *args, **kwargs)
        if len(result) == 0:
            return None
        else:
            return result[0]

    def ExecuteSingle(self, query, *args):
        with self.connection.cursor() as cursor:
            cursor.execute(query, args)

    def ReturnAll(self, query, *args, **kwargs):
        if args and kwargs:
            raise ValueError('Cannot execute query with both positional and keyword arguments')
        statementArgs = args if args else kwargs
        with self.connection.cursor() as cursor:
            cursor.execute(query, statementArgs)
            return cursor.fetchall()


class PostgresWriteConnection(PostgresConnectionBase):
    def __init__(self, user, password, database, host, port):
        super(PostgresWriteConnection, self).__init__(user, password, database, host, port)

    @stateful_database_operation
    def Commit(self):
        self.connection.commit()

    @stateful_database_operation
    def Rollback(self):
        self.connection.rollback()

    @stateful_database_operation
    def CreateSavepoint(self, name):
        super(PostgresWriteConnection, self).ExecuteSingle('SAVEPOINT {}'.format(name))

    @stateful_database_operation
    def RollbackToSavepoint(self, name):
        super(PostgresWriteConnection, self).ExecuteSingle('ROLLBACK TO SAVEPOINT {}'.format(name))

    @stateful_database_operation
    def DeleteSavepoint(self, name):
        super(PostgresWriteConnection, self).ExecuteSingle('RELEASE SAVEPOINT {}'.format(name))

    def GetCurrentPartitionBoundariesWithName(self, aggregateName):
        return self.ReturnOne(PostgresStatement.GetAggregateEventAndInstancePartitionBoundaries, aggregateName)

    def ExecuteSingle(self, query, *args):
        savepointName = 'executeSingleSavepoint'
        self.CreateSavepoint(savepointName)
        try:
            super(PostgresWriteConnection, self).ExecuteSingle(query, *args)
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Rolling back operation', 'Problem with database instruction',
                                         tags={'Query': query, 'Arguments': args})
            self.RollbackToSavepoint(savepointName)
            raise
        finally:
            self.DeleteSavepoint(savepointName)

    def ExecuteSingleAndCommit(self, query, *args):
        self.ExecuteSingle(query, *args)
        self.Commit()

    def ReturnAll(self, query, *args, **kwargs):
        if args and kwargs:
            raise ValueError('Cannot execute query with both positional and keyword arguments')
        statementArgs = args if args else kwargs
        savepointName = 'nestedTransactionSavepoint'
        self.CreateSavepoint(savepointName)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, statementArgs)
                return cursor.fetchall()
        except Exception:
            EventLogger.LogExceptionAuto(self, 'Rolling back operation', 'Problem with database instruction',
                                         tags={'Query': query, 'Arguments': statementArgs})
            self.RollbackToSavepoint(savepointName)
            raise
        finally:
            self.DeleteSavepoint(savepointName)


class PostgresReadOnlyConnection(PostgresConnectionBase):
    def __init__(self, user, password, database, host, port):
        super(PostgresReadOnlyConnection, self).__init__(user, password, database, host, port)
        self.connection.set_session(readonly=True, autocommit=True)


class PostgresEventWriteConnection(PostgresWriteConnection):
    def __init__(self, aggregateName, user, password, database, host, port):
        super(PostgresEventWriteConnection, self).__init__(user, password, database, host, port)
        self.aggregateName = aggregateName
        self.tableNames = TableNameGenerator(self.aggregateName)
        self.schemaGenerator = PLPGSQLSchemaGenerator(self.aggregateName)

    @stateful_database_operation
    def UpsertAggregateInstance(self, aggregateId, snapshot, indexValues):
        upsertStatement = PostgresStatement.UpsertAggregateInstance.format(self.tableNames.instanceBaseTableName)
        self.ExecuteSingle(upsertStatement, aggregateId, Json(indexValues), psycopg2.Binary(snapshot))

    @stateful_database_operation
    def InsertOrGetEventTypeId(self, eventType):
        selectStatement = PostgresStatement.InsertOrGetEventType.format(self.tableNames.eventTypeTableName)
        return self.ReturnOne(selectStatement, eventType)[0]

    @stateful_database_operation
    def InsertEvent(self, aggregateId, logicId, eventTypeId, eventProto, eventVersion, receivedTimestamp, processedTimestamp):
        insertStatement = PostgresStatement.InsertEvent.format(self.tableNames.eventBaseTableName)
        return self.ReturnOne(insertStatement, aggregateId, logicId, eventTypeId, eventVersion, psycopg2.Binary(eventProto), receivedTimestamp, processedTimestamp)[0]

    @stateful_database_operation
    def InsertTag(self, aggregateId, logicId, tag, tagProto, tagVersion, tagTimestamp):
        insertStatement = PostgresStatement.InsertTag.format(self.tableNames.tagTableName)
        self.ExecuteSingle(insertStatement, aggregateId, logicId, tag, psycopg2.Binary(tagProto), tagVersion, tagTimestamp)

    @stateful_database_operation
    def GetCurrentPartitionBoundaries(self):
        return self.GetCurrentPartitionBoundariesWithName(self.aggregateName)

    @stateful_database_operation
    def CreateNewEventPartitionTable(self, currentBoundary, step):
        newBoundary = currentBoundary + step
        self.ExecuteSingle(self.schemaGenerator.GenerateEventPartitionTable(currentBoundary, newBoundary))
        self.ExecuteSingle('UPDATE chronos_aggregate SET current_event_partition_boundary=%s', newBoundary)
        return newBoundary

    @stateful_database_operation
    def CreateNewInstancePartitionTable(self, currentBoundary, step):
        newBoundary = currentBoundary + step
        self.ExecuteSingle(self.schemaGenerator.GenerateAggregateInstancePartitionTable(currentBoundary, newBoundary))
        self.ExecuteSingle('UPDATE chronos_aggregate SET current_instance_partition_boundary=%s', newBoundary)
        return newBoundary

    @stateful_database_operation
    def UpdateEventPersistenceCheckpoint(self, requestId, indexValues, serializedResponse):
        insertStatement = PostgresStatement.InsertCheckpoint.format(self.tableNames.eventPersistenceStagingTableName)
        self.ExecuteSingle(insertStatement, requestId, Json(indexValues), psycopg2.Binary(serializedResponse))

    @stateless_database_operation
    def GetStagedEvents(self):
        return self.ReturnAll(PostgresStatement.GetStagedEvents.format(self.tableNames.eventPersistenceStagingTableName))

    @stateless_database_operation
    def FlushStagedEvents(self):
        self.ExecuteSingleAndCommit(PostgresStatement.DeleteAllRows.format(self.tableNames.eventPersistenceStagingTableName))


class PostgresEventReadConnection(PostgresReadOnlyConnection):
    def __init__(self, aggregateName, user, password, database, host, port):
        super(PostgresEventReadConnection, self).__init__(user, password, database, host, port)
        self.tableNames = TableNameGenerator(aggregateName)

    @stateless_database_operation
    def GetCurrentAggregateId(self):
        aggregateId = self.ReturnOne(PostgresStatement.GetAggregateId.format(self.tableNames.instanceBaseTableName))
        if aggregateId is None:
            return 0
        # NOTE: we need to return a number, not the tuple that psycopg2 returns
        return aggregateId[0]

    @stateless_database_operation
    def GetSnapshot(self, aggregateId):
        selectStatement = PostgresStatement.GetSnapshot + PostgresStatement.WhereSingleSnapshot
        return self.ReturnOne(selectStatement.format(self.tableNames.eventBaseTableName, self.tableNames.instanceBaseTableName), aggregateId)

    @stateless_database_operation
    def GetAllSnapshots(self):
        return self.ReturnAll(PostgresStatement.GetSnapshot.format(self.tableNames.eventBaseTableName, self.tableNames.instanceBaseTableName) + ';')

    @stateless_database_operation
    def GetIndexedSnapshots(self, noCaseAttributes, indexDict):
        filterStatement, queryParameters = self._buildFilterByIndexQuery(noCaseAttributes, indexDict)
        selectStatement = (PostgresStatement.GetSnapshot + filterStatement + ';').format(self.tableNames.eventBaseTableName,
                                                                                                 self.tableNames.instanceBaseTableName)
        return self.ReturnAll(selectStatement, *queryParameters)

    @stateless_database_operation
    def RetrieveAggregates(self, offset, limit, sortField, sortDescending, noCaseAttributes, indexDict):
        filterStatement, queryParameters = self._buildFilterByIndexQuery(noCaseAttributes, indexDict)
        selectStatement = PostgresStatement.GetSnapshot + filterStatement
        if not sortField or sortField == 'aggregateId':
            selectStatement += PostgresStatement.OrderByAggregateId
        else:
            selectStatement += PostgresStatement.OrderBy
            queryParameters.append(sortField)
        if sortDescending:
            selectStatement += ' DESC '
        selectStatement += PostgresStatement.LimitAndOffset + ';'
        queryParameters.extend((limit, offset))
        return (self.ReturnOne(PostgresStatement.GetCount.format(self.tableNames.instanceBaseTableName)),
               self.ReturnAll(selectStatement.format(self.tableNames.eventBaseTableName, self.tableNames.instanceBaseTableName), *queryParameters))

    @staticmethod
    def _buildFilterByIndexQuery(noCaseAttributes, indexDict):
        """Returns a tuple of string and list representing a filter statement (based on the indices provided) and list of queryParameters (based on the values of the
           indices provided) to be passed into that statement."""
        if not indexDict:
            return '', []
        aggregateId = indexDict.pop('aggregateId', None)
        filterStatement = '\nWHERE '
        if indexDict:
            filterStatement += ' AND '.join('instance.index_values->>%s = %s' for _ in range(len(indexDict)))
        # NOTE: psycopg2 needs values to be passed separately so it can perform quote escaping
        queryParameters = []
        for key, value in indexDict.iteritems():
            if key in noCaseAttributes:
                value = value.lower()
            queryParameters.extend((key, value))
        if aggregateId:
            if indexDict:
                filterStatement += ' AND '
            filterStatement += 'instance.chronos_aggregate_id=%s'
            queryParameters.append(aggregateId)
        return filterStatement, queryParameters

    @stateless_database_operation
    def GetEventsToVersion(self, aggregateId, fromVersion, toVersion):
        selectStatement = PostgresStatement.GetEventsBase + PostgresStatement.GetEventsToVersion
        return self.ReturnAll(selectStatement.format(self.tableNames.eventBaseTableName, self.tableNames.eventTypeTableName),
                              aggregateId, fromVersion, toVersion)

    @stateless_database_operation
    def GetEventsByTimestampRange(self, aggregateId, fromTimestamp, toTimestamp):
        selectStatement = PostgresStatement.GetEventsBase + PostgresStatement.GetEventsByTimestamp
        return self.ReturnAll(selectStatement.format(self.tableNames.eventBaseTableName, self.tableNames.eventTypeTableName),
                              aggregateId, fromTimestamp, toTimestamp)

    @stateless_database_operation
    def GetTag(self, aggregateId, tag):
        return self.ReturnOne(PostgresStatement.GetTag.format(self.tableNames.tagTableName), aggregateId, tag)

    @stateless_database_operation
    def GetTags(self, aggregateId):
        return self.ReturnAll(PostgresStatement.GetTagNames.format(self.tableNames.tagTableName), aggregateId)


class PostgresCrossAggregateConnection(PostgresWriteConnection):
    def __init__(self, user, password, database, host, port):
        super(PostgresCrossAggregateConnection, self).__init__(user, password, database, host, port)
        self.reader = PostgresReadOnlyConnection(user, password, database, host, port)

    @stateless_database_operation
    def GetAggregateMetadata(self, aggregateName):
        return self.reader.ReturnOne(PostgresStatement.GetAggregateMetadata, aggregateName)

    @stateless_database_operation
    def CreateInitialSchema(self, aggregateName, instancePartition, eventPartition):
        schemaGenerator = PLPGSQLSchemaGenerator(aggregateName)
        with self.connection.cursor() as cursor:
            cursor.execute(schemaGenerator.GenerateInitialSchema())
            cursor.execute(schemaGenerator.GenerateAggregateInstancePartitionTable(0, instancePartition))
            cursor.execute(schemaGenerator.GenerateEventPartitionTable(0, eventPartition))
            cursor.execute(PostgresStatement.InsertAggregateMetadata, (aggregateName, instancePartition, eventPartition))
            cursor.execute(PostgresStatement.GetAggregateMetadata, (aggregateName,))
            return cursor.fetchone()

    @stateless_database_operation
    def GetAllAggregateNames(self):
        return self.reader.ReturnAll(PostgresStatement.GetAllAggregateNames)


class PostgresLogicConnection(PostgresWriteConnection):
    def __init__(self, user, password, database, host, port):
        super(PostgresLogicConnection, self).__init__(user, password, database, host, port)
        self.reader = PostgresReadOnlyConnection(user, password, database, host, port)

    @stateless_database_operation
    def InsertAggregateLogic(self, aggregateClassId, logicVersion, pythonLogic, protoContract, constraints):
        EventLogger.LogInformationAuto(self, 'Inserting aggregate logic', tags={'AggregateClassId':aggregateClassId, 'LogicVersion':logicVersion,
                                                                                'PythonLogic': pythonLogic, 'Constraints': constraints})
        return self.ReturnOne(PostgresStatement.InsertAggregateLogic, aggregateClassId, logicVersion, pythonLogic, protoContract, constraints)

    @stateless_database_operation
    def GetLatestAggregateLogic(self, aggregateName):
        selectStatement = PostgresStatement.GetAggregateLogic + PostgresStatement.OrderByLatestAggregateLogic
        return self.reader.ReturnOne(selectStatement, aggregateName)

    @stateless_database_operation
    def GetLatestConstraints(self, aggregateName):
        latest = self.reader.ReturnOne(PostgresStatement.GetLatestConstraints, aggregateName)
        if latest is not None:
            latest = latest[0]
        return latest

    @stateless_database_operation
    def GetPreviousAggregateLogics(self, aggregateName, logicVersions):
        selectStatement = PostgresStatement.GetAggregateLogic + PostgresStatement.AndMatchesPreviousAggregateLogic
        return self.reader.ReturnAll(selectStatement, aggregateName, logicVersions)

    def _getCurrentAggregateInstancePartitionBoundaries(self, aggregateName):
        _, instancePartitionBoundary = self.GetCurrentPartitionBoundariesWithName(aggregateName) #pylint: disable=W0633
        return (instancePartitionBoundary - DEFAULTINSTANCESTEP + 1), instancePartitionBoundary

    @stateless_database_operation
    def CreateIndex(self, aggregateName, indexName, attributes):
        self._createIndexBase(aggregateName, indexName, attributes)

    @stateless_database_operation
    def CreateUniqueConstraint(self, aggregateName, indexName, attributes):
        self._createIndexBase(aggregateName, indexName, attributes, isUnique=True)

    def _createIndexBase(self, aggregateName, indexName, attributes, isUnique=False):
        constraintType = ''
        if isUnique:
            constraintType = 'UNIQUE '
        EventLogger.LogInformationAuto(self, 'Creating {}Index'.format(constraintType),
                                       tags={'aggregateName': aggregateName, 'indexName':indexName, 'attributes':attributes})
        beginning, end = self._getCurrentAggregateInstancePartitionBoundaries(aggregateName)
        selectStatement = PostgresStatement.CreateIndex.format(constraintType, indexName, TableNameGenerator(aggregateName).instanceBaseTableName, beginning, end)
        selectStatement += '(' + ','.join('(index_values->>%s)' for _ in attributes) + ');'
        self.ExecuteSingleAndCommit(selectStatement, *attributes)

    @stateless_database_operation
    def CreateNonNullConstraint(self, aggregateName, constraintName, attributes):
        EventLogger.LogInformationAuto(self, 'Creating NonNull Constraint', tags={'constraintName':constraintName})
        beginning, end = self._getCurrentAggregateInstancePartitionBoundaries(aggregateName)
        checkConstraint = '(index_values->>%s) IS NOT NULL'
        selectStatement = PostgresStatement.CreateCheckConstraint.format(TableNameGenerator(aggregateName).instanceBaseTableName,
                                                                         beginning, end, constraintName, checkConstraint)
        self.ExecuteSingleAndCommit(selectStatement, *attributes)

    @stateless_database_operation
    def DropIndex(self, indexName):
        self.ExecuteSingleAndCommit(PostgresStatement.DropIndex.format(indexName))

    @stateless_database_operation
    def DropConstraint(self, aggregateName, constraintName):
        beginning, end = self._getCurrentAggregateInstancePartitionBoundaries(aggregateName)
        self.ExecuteSingleAndCommit(PostgresStatement.DropConstraint.format(TableNameGenerator(aggregateName).instanceBaseTableName, beginning, end, constraintName))


class TableNameGenerator(object):
    def __init__(self, aggregateName):
        self.aggregateName = aggregateName
        self.eventTypeTableName = 'chronos_{}_event_type'.format(self.aggregateName.lower())
        self.instanceBaseTableName = 'chronos_{}_instance'.format(self.aggregateName.lower())
        self.eventBaseTableName = 'chronos_{}_event'.format(self.aggregateName.lower())
        self.tagTableName = 'chronos_{}_tag'.format(self.aggregateName.lower())
        self.eventPersistenceStagingTableName = 'chronos_{}_event_persistence'.format(self.aggregateName.lower())


class PLPGSQLSchemaGenerator(object):
    """Generates PLPGSQL (PostgreSQL's primary query language) for creating and updating Chronos database schemas.

    All methods of this class are functionally pure; its sole responsibility is to generate SQL to be used elsewhere
    within the PostgreSQL storage backend.
    """
    def __init__(self, aggregateName):
        """Creates a new PLPGSQLSchemaGenerator instance for an Aggregate definition at a specific instance in time.

        @param aggregateName The name of an aggregateClass
        """
        self.aggregateName = aggregateName
        self.tableNames = TableNameGenerator(aggregateName)
        self.instanceInsertFunctionName = '{}_insert()'.format(self.tableNames.instanceBaseTableName)
        self.instanceTriggerName = '{}_trigger'.format(self.tableNames.instanceBaseTableName)
        self.eventInsertFunctionName = '{}_insert()'.format(self.tableNames.eventBaseTableName)
        self.eventTriggerName = '{}_trigger'.format(self.tableNames.eventBaseTableName)

    def GenerateInitialSchema(self):
        instanceBaseTable = self.GenerateAggregateInstanceBaseTable()
        eventTypeTable = self.GenerateEventTypeTable()
        eventBaseTable = self.GenerateEventBaseTable()
        tagTable = self.GenerateTagTable()
        eventPersistenceStagingTable = self.GenerateEventPersistenceStagingTable()
        return instanceBaseTable + eventTypeTable + eventBaseTable + tagTable + eventPersistenceStagingTable

    def GenerateAggregateInstanceBaseTable(self):
        baseTableCreation = """
        CREATE TABLE {tableName}
        (
        chronos_aggregate_id bigint NOT NULL PRIMARY KEY,
        index_values json NOT NULL,
        snapshot bytea NOT NULL
        );""".format(tableName=self.tableNames.instanceBaseTableName)
        upsertFunctionCreation = self._generateAggregateInstanceUpsertFunction()
        return baseTableCreation + upsertFunctionCreation

    def _generateAggregateInstanceUpsertFunction(self):
        # NOTE: This will probably fail if we allow concurrent access to the instance table
        return """
        CREATE FUNCTION upsert_{tableName}(id bigint, index json, currentSnapshot bytea)
        RETURNS VOID AS $$
        BEGIN
          UPDATE {tableName}
          SET index_values = index, snapshot = currentSnapshot
          WHERE chronos_aggregate_id = id;
          IF NOT FOUND THEN
            INSERT INTO {tableName}(chronos_aggregate_id, index_values, snapshot) VALUES (id, index, currentSnapshot);
          END IF;
          RETURN;
        END;
        $$ LANGUAGE plpgsql;""".format(tableName=self.tableNames.instanceBaseTableName)

    def GenerateAggregateInstancePartitionTable(self, startAggregateIdExclusive, endAggregateIdInclusive):
        tableName = 'chronos_{aggregateName}_instance_{displayStartVersion}_{endVersion}'.format(aggregateName=self.aggregateName.lower(),
                                                                                                 displayStartVersion=startAggregateIdExclusive + 1,
                                                                                                 endVersion=endAggregateIdInclusive)
        partitionCreation = """
        CREATE TABLE {tableName}
        (
        UNIQUE(chronos_aggregate_id),
        CHECK(chronos_aggregate_id > {startId} AND chronos_aggregate_id <= {endId})
        ) INHERITS ({baseTableName});""".format(tableName=tableName,
                                                baseTableName=self.tableNames.instanceBaseTableName,
                                                startId=startAggregateIdExclusive,
                                                endId=endAggregateIdInclusive)
        functionCreation = self._generateAggregateInstanceInsertionFunction(tableName)
        triggerCreation = self._generateAggregateInstanceInsertionTrigger()
        return partitionCreation + functionCreation + triggerCreation

    def _generateAggregateInstanceInsertionFunction(self, tableName):
        return """
        CREATE OR REPLACE FUNCTION {instanceInsertFunctionName}
        RETURNS TRIGGER AS $$
        BEGIN
          INSERT INTO {tableName} VALUES(NEW.*);
          RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;""".format(instanceInsertFunctionName=self.instanceInsertFunctionName,
                                       tableName=tableName)

    def _generateAggregateInstanceInsertionTrigger(self):
        return """
        DROP TRIGGER IF EXISTS {instanceTriggerName} ON {instanceBaseTableName};
        CREATE TRIGGER {instanceTriggerName} BEFORE INSERT ON {instanceBaseTableName}
        FOR EACH ROW EXECUTE PROCEDURE {instanceInsertFunctionName};""".format(instanceTriggerName=self.instanceTriggerName,
                                                                               instanceBaseTableName=self.tableNames.instanceBaseTableName,
                                                                               instanceInsertFunctionName=self.instanceInsertFunctionName)

    def GenerateEventTypeTable(self):
        return """
        CREATE TABLE {tableName}
        (
        chronos_event_type_id smallserial NOT NULL PRIMARY KEY,
        event_type varchar(200) NOT NULL UNIQUE
        );""".format(tableName=self.tableNames.eventTypeTableName)

    def GenerateEventPersistenceStagingTable(self):
        return """
        CREATE TABLE {tableName}
        (
        event_persistence_id bigserial PRIMARY KEY,
        request_id bigint NOT NULL,
        index_values json NOT NULL,
        serialized_response bytea NOT NULL
        );""".format(tableName=self.tableNames.eventPersistenceStagingTableName)

    def GenerateEventBaseTable(self):
        return """
        CREATE TABLE {tableName}
        (
        chronos_event_id bigserial NOT NULL PRIMARY KEY,
        chronos_aggregate_id bigint NOT NULL,
        chronos_logic_id smallint NOT NULL,
        chronos_event_type_id smallint NOT NULL,
        event_version bigint NOT NULL,
        event_proto bytea NOT NULL,
        received_timestamp bigint NOT NULL,
        processed_timestamp bigint NOT NULL,
        CHECK (event_version > 0 AND received_timestamp > 0 AND processed_timestamp > 0)
        );""".format(tableName=self.tableNames.eventBaseTableName)

    def GenerateEventPartitionTable(self, startVersionExclusive, endVersionInclusive):
        tableName = 'chronos_{aggregateName}_event_{displayStartVersion}_{endVersion}'.format(aggregateName=self.aggregateName.lower(),
                                                                                              displayStartVersion=startVersionExclusive + 1,
                                                                                              endVersion=endVersionInclusive)
        partitionCreation = """
        CREATE TABLE {tableName}
        (
        UNIQUE(chronos_event_id),
        UNIQUE(chronos_aggregate_id, event_version),
        FOREIGN KEY (chronos_logic_id) REFERENCES chronos_logic,
        FOREIGN KEY (chronos_event_type_id) REFERENCES {eventTypeTableName},
        CHECK(event_version > {startVersion} AND event_version <= {endVersion})
        ) INHERITS ({baseTableName});
        CREATE INDEX ON {tableName} (chronos_aggregate_id, processed_timestamp);
        CREATE INDEX ON {tableName} (chronos_aggregate_id, event_version);""".format(tableName=tableName,
                                                                                              baseTableName=self.tableNames.eventBaseTableName,
                                                                                              startVersion=startVersionExclusive,
                                                                                              endVersion=endVersionInclusive,
                                                                                              eventTypeTableName=self.tableNames.eventTypeTableName)
        functionCreation = self._generateEventInsertionFunction(tableName)
        triggerCreation = self._generateEventInsertionTrigger()
        return partitionCreation + functionCreation + triggerCreation

    def _generateEventInsertionFunction(self, tableName):
        return """
        CREATE OR REPLACE FUNCTION {eventInsertFunctionName}
        RETURNS TRIGGER AS $$
        BEGIN
          INSERT INTO {tableName} VALUES(NEW.*);
          RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;""".format(eventInsertFunctionName=self.eventInsertFunctionName,
                                       tableName=tableName)

    def _generateEventInsertionTrigger(self):
        return """
        DROP TRIGGER IF EXISTS {eventTriggerName} ON {eventBaseTableName};
        CREATE TRIGGER {eventTriggerName} BEFORE INSERT ON {eventBaseTableName}
        FOR EACH ROW EXECUTE PROCEDURE {eventInsertFunctionName};""".format(eventTriggerName=self.eventTriggerName,
                                                                            eventBaseTableName=self.tableNames.eventBaseTableName,
                                                                            eventInsertFunctionName=self.eventInsertFunctionName)

    def GenerateTagTable(self):
        return """
        CREATE TABLE {tagTableName}
        (
        chronos_tag_id bigserial NOT NULL PRIMARY KEY,
        chronos_aggregate_id bigint NOT NULL,
        chronos_logic_id smallint NOT NULL REFERENCES chronos_logic,
        tag text NOT NULL,
        tag_proto bytea NOT NULL,
        tag_version bigint NOT NULL,
        tag_timestamp bigint NOT NULL,
        UNIQUE (chronos_aggregate_id, tag),
        CHECK (tag_version > 0 AND tag_timestamp > 0)
        );""".format(tagTableName=self.tableNames.tagTableName)
