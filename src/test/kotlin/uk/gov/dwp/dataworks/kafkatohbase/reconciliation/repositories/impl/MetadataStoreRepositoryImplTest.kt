package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import java.sql.*

class MetadataStoreRepositoryImplTest {

    @Test
    fun testReconcileRecordsWithAutoCommit() {
        testAutoCommit(true)
    }

    @Test
    fun testReconcileRecordsWithoutAutoCommit() {
        testAutoCommit(false)
    }

    fun testAutoCommit(autoOn: Boolean) {
        val statement = mock<PreparedStatement>()

        val connection = mock<Connection> {
            on { prepareStatement(any()) } doReturn statement
            on { autoCommit } doReturn autoOn
        }

        val records = (1 .. 100).map {
            UnreconciledRecord(it, "db.database.collection${it % 3}", "hbase_id_$it", (it * 10).toLong())
        }

        val repository = MetadataStoreRepositoryImpl(connection, "ucfs")
        repository.reconcileRecords(records)

        val sqlCaptor = argumentCaptor<String>()
        verify(connection, times(1)).prepareStatement(sqlCaptor.capture())
        verify(connection, times(1)).autoCommit

        if (autoOn) {
            verify(connection, times(0)).commit()
        } else {
            verify(connection, times(1)).commit()
        }

        verifyNoMoreInteractions(connection)
        assertEquals("""
                        UPDATE ucfs
                        SET reconciled_result=true, reconciled_timestamp=CURRENT_TIMESTAMP
                        WHERE id = ?
                     """.trimIndent(), sqlCaptor.firstValue)

        verify(statement, times(100)).addBatch()
        val positionCaptor = argumentCaptor<Int>()
        val idCaptor = argumentCaptor<Int>()
        verify(statement, times(100)).setInt(positionCaptor.capture(), idCaptor.capture())

        positionCaptor.allValues.forEach {
            assertEquals(1, it)
        }

        idCaptor.allValues.forEachIndexed { index, id ->
            assertEquals(index + 1, id)
        }
        verify(statement, times(1)).executeBatch()
        verifyNoMoreInteractions(statement)
    }

    @Test
    fun testReconcileNoRecords() {
        val statement = mock<PreparedStatement>()
        val connection = mock<Connection>()
        val repository = MetadataStoreRepositoryImpl(connection, "ucfs")
        repository.reconcileRecords(listOf<UnreconciledRecord>())
        verifyZeroInteractions(connection)
        verifyZeroInteractions(statement)
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

        val repository = MetadataStoreRepositoryImpl(connection, "ucfs")
        val grouped = repository.groupedUnreconciledRecords()

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
    fun givenALimitExistsForRecordsReturnedWhenIRequestAListOfRecordsFromMetadataStoreThenTheFirstValueContainsLimit() {

        val resultSet = mock<ResultSet> {
            on { next() } doReturn false
        }

        val statement = mock<Statement> {
            on { executeQuery(any()) } doReturn resultSet
        }

        val metadataStoreConnection = mock<Connection> {
            on { createStatement() } doReturn statement
        }

        val metadataStoreRepository = MetadataStoreRepositoryImpl(metadataStoreConnection, "ucfs")

        metadataStoreRepository.fetchUnreconciledRecords()

        verify(metadataStoreConnection, times(1)).createStatement()
        val captor = argumentCaptor<String>()
        verify(statement, times(1)).executeQuery(captor.capture())
        assert(captor.firstValue.contains("LIMIT"))
    }

    @Test
    fun givenRecordExistsToBeReconciledWhenRequestingToReconcileATopicNameThenTheTopicNameIsMarkedAsReconciled() {

        val topicName = "to:reconcile"
        val rowsUpdated = 1

        val statement = mock<PreparedStatement> {
            on {
                executeUpdate(any())
            } doReturn rowsUpdated
        }

        val metadataStoreConnection = mock<Connection> {
            on { prepareStatement(any()) } doReturn statement
        }

        val metadataStoreRepository = MetadataStoreRepositoryImpl(metadataStoreConnection, "ucfs")

        val hbaseId = "hbase-id"
        val hbaseTimestamp = 100L
        metadataStoreRepository.reconcileRecord(topicName, hbaseId, hbaseTimestamp)

        verify(metadataStoreConnection, times(1)).prepareStatement(any())
        verify(statement, times(1)).setString(1, topicName)
    }
}
