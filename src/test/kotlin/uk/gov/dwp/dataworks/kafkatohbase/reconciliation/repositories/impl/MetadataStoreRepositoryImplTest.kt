package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.*

class MetadataStoreRepositoryImplTest {

    @Test
    fun testReconciledRecords() {
        val resultSet = mock<ResultSet> {
            on { next() } doReturnConsecutively (1..101).map {it < 100}
            on { getInt("id") } doReturnConsecutively  (1..100).toList()
            on { getString("topic_name") } doReturnConsecutively (1..100).map { "db.database.collection${it % 3}" }
            on { getString("hbase_id") } doReturnConsecutively (1..100).map { "hbase_id_$it" }
            on { getTimestamp(("hbase_timestamp"))} doReturnConsecutively (1..100).map { Timestamp(it.toLong()) }
        }

        val statement = mock<PreparedStatement> {
            on { executeQuery() } doReturn resultSet
        }

        val metadataStoreConnection = mock<Connection> {
            on { prepareStatement(any()) } doReturn statement
        }

        val metadataStoreRepository = MetadataStoreRepositoryImpl(metadataStoreConnection, "ucfs")
        val grouped = metadataStoreRepository.groupedUnreconciledRecords()
        assertEquals(3, grouped.size)
        grouped.keys.sorted().forEachIndexed { index, key -> assertEquals("db.database.collection$index", key) }
        grouped.forEach { (_, records) -> assertEquals(33, records.size) }
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
