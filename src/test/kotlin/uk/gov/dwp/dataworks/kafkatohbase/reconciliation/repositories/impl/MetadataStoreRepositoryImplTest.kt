package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import java.sql.*

class MetadataStoreRepositoryImplTest {

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
            mock<Connection>().run {
                repository(this).reconcileRecords(listOf())
                verifyZeroInteractions(this)
            }


    @Test
    fun testGroupedUnreconciledRecords() {
        val resultSet = mock<ResultSet> {
            on { next() } doReturnConsecutively (1..101).map {it < 100}
            on { getInt("id") } doReturnConsecutively  (1..100).toList()
            on { getString("topic_name") } doReturnConsecutively (1..100).map { "db.database.collection${it % 3}" }
            on { getString("hbase_id") } doReturnConsecutively (1..100).map { "hbase_id_$it" }
            on { getTimestamp("hbase_timestamp") } doReturnConsecutively (1..100).map { Timestamp(it.toLong()) }
        }

        val statement = mock<PreparedStatement> {
            on { executeQuery() } doReturn resultSet
        }

        val connection = mock<Connection> {
            on { prepareStatement(any()) } doReturn statement
        }

        val grouped = repository(connection).groupedUnreconciledRecords(10, "MINUTE")
        assertEquals(3, grouped.size)
        grouped.keys.sorted().forEachIndexed { index, key -> assertEquals("db.database.collection$index", key) }
        grouped.forEach { (_, records) -> assertEquals(33, records.size) }
        val sqlCaptor = argumentCaptor<String>()
        verify(connection, times(1)).prepareStatement(sqlCaptor.capture())
        verifyNoMoreInteractions(connection)
        assertTrue(sqlCaptor.firstValue.contains("reconciled_result = false"))
        assertTrue(sqlCaptor.firstValue.contains("LIMIT"))

        verify(statement, times(1)).executeQuery()
        verifyNoMoreInteractions(statement)

        verify(resultSet, times(100)).next()
        verify(resultSet, times(99)).getInt("id")
        verify(resultSet, times(99)).getString("topic_name")
        verify(resultSet, times(99)).getString("hbase_id")
        verify(resultSet, times(99)).getTimestamp("hbase_timestamp")
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

        val metadataStoreRepository = MetadataStoreRepositoryImpl(metadataStoreConnection, "ucfs")

        metadataStoreRepository.deleteRecordsOlderThanPeriod(trimReconciledScale, trimReconciledUnit)

        verify(metadataStoreConnection, times(1)).createStatement()
        verify(statement, times(1)).executeUpdate(any())
    }

    private fun testAutoCommit(autoOn: Boolean) {
        val statement = mock<PreparedStatement>()
        val connection = connection(statement, autoOn)
        repository(connection).reconcileRecords(unreconciledRecords())
        verifyBatchedConnectionInteractions(connection, autoOn, true)
        verifyBatchedStatementInteractions(statement)
    }

    private fun testRollback(autoOn: Boolean) {
        val statement = mock<PreparedStatement> {
            on { executeBatch() } doThrow SQLException("Error updating")
        }
        val connection = connection(statement, autoOn)
        repository(connection).reconcileRecords(unreconciledRecords())
        verifyBatchedConnectionInteractions(connection, autoOn, false)
        verifyBatchedStatementInteractions(statement)
    }

    private fun verifyBatchedConnectionInteractions(connection: Connection, autoOn: Boolean, updateSucceeds: Boolean) {
        val sqlCaptor = argumentCaptor<String>()
        verify(connection, times(1)).prepareStatement(sqlCaptor.capture())
        verify(connection, times(1)).autoCommit
        if (!autoOn) {
            if (updateSucceeds) {
                verify(connection, times(1)).commit()
            }
            else {
                verify(connection, times(1)).rollback()
            }
        }
        assertEquals("""
                        UPDATE ucfs
                        SET reconciled_result=true, reconciled_timestamp=CURRENT_TIMESTAMP
                        WHERE id = ?""".trimIndent(), sqlCaptor.firstValue)
        verifyNoMoreInteractions(connection)
    }

    private fun verifyBatchedStatementInteractions(statement: PreparedStatement) {
        verify(statement, times(100)).addBatch()
        val positionCaptor = argumentCaptor<Int>()
        val idCaptor = argumentCaptor<Int>()
        verify(statement, times(100)).setInt(positionCaptor.capture(), idCaptor.capture())
        positionCaptor.allValues.forEach { assertEquals(1, it) }
        idCaptor.allValues.forEachIndexed { index, id -> assertEquals(index + 1, id) }
        verify(statement, times(1)).executeBatch()
        verifyNoMoreInteractions(statement)
    }

    private fun connection(statement: PreparedStatement, autoOn: Boolean) =
        mock<Connection> {
            on { prepareStatement(any()) } doReturn statement
            on { autoCommit } doReturn autoOn
        }

    private fun unreconciledRecords(): List<UnreconciledRecord> =
        (1..100).map {
            UnreconciledRecord(it, "db.database.collection${it % 3}", "hbase_id_$it", (it * 10).toLong())
        }

    private fun repository(connection: Connection): MetadataStoreRepositoryImpl =
        MetadataStoreRepositoryImpl(connection, "ucfs")
}
