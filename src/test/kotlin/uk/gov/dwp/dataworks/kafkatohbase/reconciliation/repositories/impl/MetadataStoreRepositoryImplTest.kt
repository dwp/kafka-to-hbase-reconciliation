package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl

import com.nhaarman.mockitokotlin2.*
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.suppliers.ConnectionSupplier
import java.sql.*
import kotlin.time.ExperimentalTime


@ExperimentalTime
class MetadataStoreRepositoryImplTest {

    @Test
    fun testRecordLastCheckedWithNoPartitioning() {
        val sql = """UPDATE ucfs
                    |SET last_checked_timestamp=CURRENT_TIMESTAMP, last_checked_timestamp=CURRENT_TIMESTAMP
                    |WHERE id = ?
                    """.trimMargin()
        val statementConnection = mock<Connection>()
        val statement = mock<PreparedStatement> {
            on { connection } doReturn statementConnection
        }
        val metadataStoreConnection = mock<Connection> {
            on { prepareStatement(sql) } doReturn statement
        }
        val connectionSupplier = connectionSupplier(listOf(metadataStoreConnection))
        val metadataStoreRepository =
                MetadataStoreRepositoryImpl(connectionSupplier, "ucfs", 10, 100, 10, "NOT_SET")

        metadataStoreRepository.recordLastChecked(unreconciledRecords())

        verify(connectionSupplier, times(1)).connection()
        verifyNoMoreInteractions(connectionSupplier)

        verify(metadataStoreConnection, times(1)).prepareStatement(sql)
        verify(metadataStoreConnection, times(1)).close()
        verifyNoMoreInteractions(metadataStoreConnection)

        val idCaptor = argumentCaptor<Int>()
        val indexCaptor = argumentCaptor<Int>()
        verify(statement, times(100)).setInt(indexCaptor.capture(), idCaptor.capture())
        verify(statement, times(100)).addBatch()
        verify(statement, times(1)).executeBatch()
        verify(statement, times(1)).connection
        verify(statement, times(1)).close()
        idCaptor.allValues shouldHaveSize 100
        idCaptor.allValues.forEachIndexed { index, id -> id shouldBe index + 1 }
        indexCaptor.allValues shouldHaveSize 100
        indexCaptor.allValues.forEach { it shouldBe 1 }
        verifyNoMoreInteractions(statement)

        verify(statementConnection, times(1)).autoCommit
        verify(statementConnection, times(1)).commit()
        verifyNoMoreInteractions(statementConnection)
    }

    @Test
    fun testDeleteAll() {
        val deleteLimit = 10
        val sql = """DELETE FROM ucfs
        |WHERE reconciled_result = TRUE 
        |LIMIT $deleteLimit""".trimMargin()

        val deleteCounts = listOf(10, 10, 10, 8)
        val statement = mock<Statement> {
            on { executeUpdate(sql) } doReturnConsecutively deleteCounts
        }

        val autoCommitEnabled = false
        val metadataStoreConnection = mock<Connection> {
            on { createStatement() } doReturn statement
            on { autoCommit } doReturn autoCommitEnabled
        }

        val connectionSupplier = connectionSupplier(listOf(metadataStoreConnection))
        val metadataStoreRepository =
                MetadataStoreRepositoryImpl(connectionSupplier, "ucfs", 10, 100,
                        deleteLimit, "NOT_SET")

        val totalDeletes = metadataStoreRepository.deleteAllReconciledRecords()
        totalDeletes shouldBe deleteCounts.sum()
        verifySupplierInteractions(connectionSupplier, deleteCounts.size)

        verify(metadataStoreConnection, times(deleteCounts.size)).autoCommit
        verify(metadataStoreConnection, times(deleteCounts.size)).commit()
        verifyConnectionInteractions(metadataStoreConnection, deleteCounts.size)
        verify(statement, times(deleteCounts.size)).executeUpdate(sql)
        verifyCommonStatementInteractions(statement, deleteCounts.size)
    }

    @Test
    fun testOptimize() {

        val sql = "OPTIMIZE TABLE ucfs"

        val statement = mock<Statement> {
            on { execute(sql) } doReturn true
        }

        val metadataStoreConnection = mock<Connection> {
            on { createStatement() } doReturn statement
        }

        val connectionSupplier = connectionSupplier(listOf(metadataStoreConnection))
        val metadataStoreRepository =
                MetadataStoreRepositoryImpl(connectionSupplier, "ucfs", 10, 100, 10, "NOT_SET")

        metadataStoreRepository.optimizeTable()

        verifySupplierInteractions(connectionSupplier)
        verifyConnectionInteractions(metadataStoreConnection)
        verify(statement, times(1)).execute(sql)
        verifyCommonStatementInteractions(statement)
    }

    @Test
    fun testReconcileRecordsWithAutoCommit() = testAutoCommit(true)

    @Test
    fun testReconcileRecordsWithoutAutoCommit() = testAutoCommit(false)

    @Test
    fun testReconcileRecordsExceptionWithAutoCommit() = testRollback(true)

    @Test
    fun testReconcileRecordsExceptionWithoutAutoCommit() = testRollback(false)

    @Test
    fun testReconcileNoRecords() =
            mock<ConnectionSupplier>().run {
                repository(this).reconcileRecords(listOf())
                verifyZeroInteractions(this)
            }

    @Test
    fun testReconcileOneRecord() {
        val statementConnection = mock<Connection>()
        val statement = mock<PreparedStatement> {
            on { connection } doReturn statementConnection
        }

        val connection = mock<Connection> {
            on { prepareStatement(any()) } doReturn statement
            on { autoCommit } doReturn false
        }

        val connectionSupplier = connectionSupplier(listOf(connection))

        repository(connectionSupplier).reconcileRecords(listOf(
                UnreconciledRecord(1, "db.database.collection1", "hbase_id_1", (10).toLong())))
        verifySupplierInteractions(connectionSupplier)
        verify(connection, times(1)).prepareStatement(any())
        verify(connection, times(1)).close()
        verifyNoMoreInteractions(connection)

        verify(statement, times(1)).setInt(1, 1)
        verify(statement, times(1)).addBatch()
        verify(statement, times(1)).executeBatch()
        verify(statement, times(1)).close()
        verify(statement, times(1)).connection
        verifyNoMoreInteractions(statement)

        verify(statementConnection, times(1)).autoCommit
        verify(statementConnection, times(1)).commit()
        verifyNoMoreInteractions(statementConnection)
    }

    @Test
    fun testReconcileOneRecordWithMultiplePartition() {
        val statementConnection = mock<Connection>()
        val statement = mock<PreparedStatement> {
            on { connection } doReturn statementConnection
        }

        val connection = mock<Connection> {
            on { prepareStatement(any()) } doReturn statement
            on { autoCommit } doReturn false
        }

        val connectionSupplier = connectionSupplier(listOf(connection))

        repositoryWithPartition(connectionSupplier).reconcileRecords(listOf(
                UnreconciledRecord(1, "db.database.collection1", "hbase_id_1", (10).toLong())))

        verifySupplierInteractions(connectionSupplier)
        verify(connection, times(1)).prepareStatement(any())
        verify(connection, times(1)).close()
        verifyNoMoreInteractions(connection)

        verify(statement, times(1)).setInt(1, 1)
        verify(statement, times(1)).addBatch()
        verify(statement, times(1)).executeBatch()
        verify(statement, times(1)).close()
        verify(statement, times(1)).connection
        verifyNoMoreInteractions(statement)

        verify(statementConnection, times(1)).autoCommit
        verify(statementConnection, times(1)).commit()
        verifyNoMoreInteractions(statementConnection)
    }

    private fun verifySupplierInteractions(connectionSupplier: ConnectionSupplier, numInvocations: Int = 1) {
        verify(connectionSupplier, times(numInvocations)).connection()
        verifyNoMoreInteractions(connectionSupplier)
    }

    @Test
    fun testGroupedUnreconciledRecordsWithoutPartitions() {

        val resultSet = mock<ResultSet> {
            on { next() } doReturnConsecutively (1..101).map { it < 100 }
            on { getInt("id") } doReturnConsecutively (1..100).toList()
            on { getString("topic_name") } doReturnConsecutively (1..100).map { "db.database.collection${it % 3}" }
            on { getString("hbase_id") } doReturnConsecutively (1..100).map { "hbase_id_$it" }
            on { getTimestamp("hbase_timestamp") } doReturnConsecutively (1..100).map { Timestamp(it.toLong()) }
        }

        val statement = mock<PreparedStatement> { on { executeQuery() } doReturn resultSet }
        val connection = mock<Connection> { on { prepareStatement(any()) } doReturn statement }
        val connectionSupplier = connectionSupplier(listOf(connection))

        val minAgeSize = 10
        val minAgeUnit = "MINUTE"
        val lastCheckedSize = 5
        val lastCheckedUnit = "HOUR"

        val grouped = repository(connectionSupplier).groupedUnreconciledRecords(minAgeSize, minAgeUnit, lastCheckedSize, lastCheckedUnit)

        assertEquals(3, grouped.size)
        grouped.keys.sorted().forEachIndexed { index, key -> assertEquals("db.database.collection$index", key) }
        grouped.forEach { (_, records) -> assertEquals(33, records.size) }
        val sqlCaptor = argumentCaptor<String>()

        verify(connectionSupplier, times(1)).connection()
        verify(connection, times(1)).prepareStatement(sqlCaptor.capture())
        verify(connection, times(1)).close()
        verifyNoMoreInteractions(connection)
        assertTrue(sqlCaptor.firstValue.contains("reconciled_result = false"))
        assertTrue(sqlCaptor.firstValue.contains("LIMIT"))
        assertTrue(sqlCaptor.firstValue.contains("write_timestamp < CURRENT_TIMESTAMP - INTERVAL $minAgeSize $minAgeUnit"))
        assertFalse(sqlCaptor.firstValue.contains("PARTITION"))

        verify(statement, times(1)).executeQuery()
        verify(statement, times(1)).close()
        verifyNoMoreInteractions(statement)

        verify(resultSet, times(100)).next()
        verify(resultSet, times(99)).getInt("id")
        verify(resultSet, times(99)).getString("topic_name")
        verify(resultSet, times(99)).getString("hbase_id")
        verify(resultSet, times(99)).getLong("hbase_timestamp")
        verify(resultSet, times(1)).close()

        verifyNoMoreInteractions(resultSet)
    }

    @Test
    fun testGroupedUnreconciledRecordsWithMultiplePartitions() {

        val resultSet = mock<ResultSet> {
            on { next() } doReturnConsecutively (1..101).map { it < 100 }
            on { getInt("id") } doReturnConsecutively (1..100).toList()
            on { getString("topic_name") } doReturnConsecutively (1..100).map { "db.database.collection${it % 3}" }
            on { getString("hbase_id") } doReturnConsecutively (1..100).map { "hbase_id_$it" }
            on { getTimestamp("hbase_timestamp") } doReturnConsecutively (1..100).map { Timestamp(it.toLong()) }
        }

        val statement = mock<PreparedStatement> { on { executeQuery() } doReturn resultSet }
        val connection = mock<Connection> { on { prepareStatement(any()) } doReturn statement }
        val connectionSupplier = connectionSupplier(listOf(connection))

        val minAgeSize = 10
        val minAgeUnit = "MINUTE"
        val lastCheckedSize = 5
        val lastCheckedUnit = "HOUR"

        val grouped = repositoryWithPartition(connectionSupplier).groupedUnreconciledRecords(minAgeSize, minAgeUnit, lastCheckedSize, lastCheckedUnit)

        assertEquals(3, grouped.size)
        grouped.keys.sorted().forEachIndexed { index, key -> assertEquals("db.database.collection$index", key) }
        grouped.forEach { (_, records) -> assertEquals(33, records.size) }
        val sqlCaptor = argumentCaptor<String>()

        verify(connectionSupplier, times(1)).connection()
        verify(connection, times(1)).prepareStatement(sqlCaptor.capture())
        verify(connection, times(1)).close()
        verifyNoMoreInteractions(connection)
        assertTrue(sqlCaptor.firstValue.contains("reconciled_result = false"))
        assertTrue(sqlCaptor.firstValue.contains("LIMIT"))
        assertTrue(sqlCaptor.firstValue.contains("write_timestamp < CURRENT_TIMESTAMP - INTERVAL $minAgeSize $minAgeUnit"))
        assertTrue(sqlCaptor.firstValue.contains("PARTITION (p0,p1,p2,p3"))

        verify(statement, times(1)).executeQuery()
        verify(statement, times(1)).close()
        verifyNoMoreInteractions(statement)

        verify(resultSet, times(100)).next()
        verify(resultSet, times(99)).getInt("id")
        verify(resultSet, times(99)).getString("topic_name")
        verify(resultSet, times(99)).getString("hbase_id")
        verify(resultSet, times(99)).getLong("hbase_timestamp")
        verify(resultSet, times(1)).close()

        verifyNoMoreInteractions(resultSet)
    }

    @Test
    fun givenRecordsOlderThanScaleAndUnitExistWhenRequestingToTrimRecordsThenRecordsAreDeleted() {
        val trimReconciledScale = "1"
        val trimReconciledUnit = "DAY"

        val rowsUpdated = 1

        val statement = mock<Statement> {
            on {
                executeUpdate(any())
            } doReturn rowsUpdated
        }

        val metadataStoreConnection = mock<Connection> {
            on { createStatement() } doReturn statement
        }

        val connectionSupplier = connectionSupplier(listOf(metadataStoreConnection))
        val metadataStoreRepository =
                MetadataStoreRepositoryImpl(connectionSupplier, "ucfs", 10, 100, 10, "NOT_SET")

        metadataStoreRepository.deleteRecordsOlderThanPeriod(trimReconciledScale, trimReconciledUnit)
        verify(connectionSupplier, times(1)).connection()
        verifyConnectionInteractions(metadataStoreConnection)
        verify(statement, times(1)).executeUpdate(any())
        verifyCommonStatementInteractions(statement)
    }

    private fun verifyCommonStatementInteractions(statement: Statement, numInvocations: Int = 1) {
        verify(statement, times(numInvocations)).close()
        verifyNoMoreInteractions(statement)
    }

    private fun verifyConnectionInteractions(metadataStoreConnection: Connection, numInvocations: Int = 1) {
        verify(metadataStoreConnection, times(numInvocations)).createStatement()
        verify(metadataStoreConnection, times(numInvocations)).close()
        verifyNoMoreInteractions(metadataStoreConnection)
    }

    private fun testAutoCommit(autoOn: Boolean) {
        val statementConnections = (0 until 10).map { statementConnection(autoOn) }
        val statements = (0 until 10).map { index ->
            mock<PreparedStatement> {
                on { connection } doReturn statementConnections[index]
            }
        }

        val connections = (0 until 10).map { connection(statements[it], autoOn) }
        val connectionSupplier = connectionSupplier(connections)
        repository(connectionSupplier).reconcileRecords(unreconciledRecords())
        connections.forEach(::verifyBatchedConnectionInteractions)
        statements.forEach(::verifyBatchedStatementInteractions)
        statementConnections.forEach { verifyStatementConnectionInteractions(it, autoOn, true) }
    }

    private fun testRollback(autoOn: Boolean) {
        val statementConnections = (0 until 10).map { statementConnection(autoOn) }
        val statements = (0 until 10).map { index ->
            mock<PreparedStatement> {
                on { executeBatch() } doThrow SQLException("Error updating")
                on { connection } doReturn statementConnections[index]
            }
        }
        val connections = (0 until 10).map { connection(statements[it], autoOn) }
        val connectionSupplier = connectionSupplier(connections)
        repository(connectionSupplier).reconcileRecords(unreconciledRecords())
        connections.forEach(::verifyBatchedConnectionInteractions)
        statements.forEach(::verifyBatchedStatementInteractions)
        statementConnections.forEach { verifyStatementConnectionInteractions(it, autoOn, false) }
    }

    private fun verifyBatchedConnectionInteractions(connection: Connection) {
        val sqlCaptor = argumentCaptor<String>()
        verify(connection, times(1)).prepareStatement(sqlCaptor.capture())
        assertEquals("""
                        UPDATE ucfs
                        SET reconciled_result=true, reconciled_timestamp=CURRENT_TIMESTAMP, last_checked_timestamp=CURRENT_TIMESTAMP
                        WHERE id = ?""".trimIndent(), sqlCaptor.firstValue.trim())
        verify(connection, times(1)).close()
        verifyNoMoreInteractions(connection)
    }

    private fun verifyStatementConnectionInteractions(connection: Connection, autoOn: Boolean, updateSucceeds: Boolean) {
        verify(connection, times(1)).autoCommit

        if (!autoOn) {
            if (updateSucceeds) {
                verify(connection, times(1)).commit()
            } else {
                verify(connection, times(1)).rollback()
            }
        }
        verifyNoMoreInteractions(connection)
    }

    private fun verifyBatchedStatementInteractions(statement: PreparedStatement) {
        verify(statement, times(10)).addBatch()
        val positionCaptor = argumentCaptor<Int>()
        val idCaptor = argumentCaptor<Int>()
        verify(statement, times(10)).setInt(positionCaptor.capture(), idCaptor.capture())
        positionCaptor.allValues.forEach { assertEquals(1, it) }
        idCaptor.allValues.forEachIndexed { index, id -> assertEquals((index + 1) % 10, id % 10) }
        verify(statement, times(1)).executeBatch()
        verify(statement, times(1)).close()
        verify(statement, times(1)).connection
        verifyNoMoreInteractions(statement)
    }

    private fun connectionSupplier(connections: List<Connection>) =
            mock<ConnectionSupplier> {
                on { connection() } doReturnConsecutively connections
            }

    private fun connection(statement: PreparedStatement, autoOn: Boolean) =
            mock<Connection> {
                on { prepareStatement(any()) } doReturn statement
                on { autoCommit } doReturn autoOn
            }

    private fun statementConnection(autoOn: Boolean) =
            mock<Connection> {
                on { autoCommit } doReturn autoOn
            }

    private fun unreconciledRecords(): List<UnreconciledRecord> =
            (1..100).map {
                UnreconciledRecord(it, "db.database.collection${it % 3}", "hbase_id_$it", (it * 10).toLong())
            }

    private fun repository(connectionSupplier: ConnectionSupplier): MetadataStoreRepositoryImpl =
            MetadataStoreRepositoryImpl(connectionSupplier, "ucfs", 10, 100, 10, "NOT_SET")

    private fun repositoryWithPartition(connectionSupplier: ConnectionSupplier): MetadataStoreRepositoryImpl =
            MetadataStoreRepositoryImpl(connectionSupplier, "ucfs", 10, 100, 10, "p0,p1,p2,p3")
}
