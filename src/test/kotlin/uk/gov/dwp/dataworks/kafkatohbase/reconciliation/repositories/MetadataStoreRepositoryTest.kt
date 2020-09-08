package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement

class MetadataStoreRepositoryTest {

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

        val metadataStoreRepository = MetadataStoreRepository(metadataStoreConnection, "ucfs")

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

        val metadataStoreRepository = MetadataStoreRepository(metadataStoreConnection, "ucfs")

        val hbaseId = "hbase-id"
        val hbaseTimestamp = 100L
        metadataStoreRepository.reconcileRecord(topicName, hbaseId, hbaseTimestamp)

        verify(metadataStoreConnection, times(1)).prepareStatement(any())
        verify(statement, times(1)).setString(1, topicName)
    }

    @Test
    fun givenRecordsOlderThanScaleAndUnitExistWhenRequestingToTrimRecordsThenRecordsAreDeleted() {

        val rowsUpdated = 1

        val statement = mock<Statement> {
            on {
                executeUpdate(any())
            } doReturn rowsUpdated
        }

        val metadataStoreConnection = mock<Connection> {
            on { createStatement() } doReturn statement
        }

        val metadataStoreRepository = MetadataStoreRepository(metadataStoreConnection, "ucfs")

        metadataStoreRepository.deleteRecordsOlderThanPeriod()

        verify(metadataStoreConnection, times(1)).createStatement()
        verify(statement, times(1)).executeUpdate(any())
    }
}
