package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.junit4.SpringRunner
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
    private lateinit var metadataStoreConnection: Connection

    // limit the number of records returned when querying metadata store
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
        given(metadataStoreConnection.createStatement()).willReturn(statement)
        metadataStoreRepository.fetchUnreconciledRecords()

        verify(metadataStoreConnection, times(1)).createStatement()
        val captor = argumentCaptor<String>()
        verify(statement, times(1)).executeQuery(captor.capture())
        assert(captor.firstValue.contains("LIMIT"))
    }

    @Test
    fun reconcileRecordsNotReturnedFromMetadataStore() {
        val pattern = Regex("""(WHERE|AND) reconciled_result = false""")
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
        given(metadataStoreConnection.createStatement()).willReturn(statement)
        metadataStoreRepository.fetchUnreconciledRecords()

        verify(metadataStoreConnection, times(1)).createStatement()
        val captor = argumentCaptor<String>()
        verify(statement, times(1)).executeQuery(captor.capture())
        assert(captor.firstValue.contains(pattern))
    }
}
