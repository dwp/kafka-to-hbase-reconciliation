package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.junit4.SpringRunner
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.MetadataStoreConfiguration
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [MetadataStoreRepository::class])
class MetadataStoreRepositoryTest {

    @SpyBean
    @Autowired
    private lateinit var metadataStoreRepository: MetadataStoreRepository

    @MockBean
    private lateinit var metadataStoreConfiguration: MetadataStoreConfiguration

    @Test
    fun givenALimitExistsForRecordsReturnedWhenIRequestAListOfRecordsFromMetadataStoreThenTheFirstValueContainsLimit() {

        val resultSet = mock<ResultSet> {
            on {
                next()
            } doReturnConsecutively listOf(false)
        }

        val statement = mock<Statement> {
            on {
                executeQuery(any())
            } doReturn resultSet
        }

        val metadataStoreConnection = mock<Connection> {
            on { createStatement() } doReturn statement
        }

        whenever(metadataStoreConfiguration.metadataStoreConnection()).thenReturn(metadataStoreConnection)

        whenever(metadataStoreConfiguration.table).thenReturn("ucfs")

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

        val statement = mock<Statement> {
            on {
                executeUpdate(any())
            } doReturn rowsUpdated
        }

        val metadataStoreConnection = mock<Connection> {
            on { createStatement() } doReturn statement
        }

        whenever(metadataStoreConfiguration.metadataStoreConnection()).thenReturn(metadataStoreConnection)

        whenever(metadataStoreConfiguration.table).thenReturn("ucfs")

        TODO("NOT IMPLEMENTED")
//        metadataStoreRepository.reconcileRecord(topicName, hbaseId, hbaseTimestamp.time)
//
//        verify(metadataStoreConnection, times(1)).createStatement()
//        verify(statement, times(1)).executeUpdate(any())
    }
}
