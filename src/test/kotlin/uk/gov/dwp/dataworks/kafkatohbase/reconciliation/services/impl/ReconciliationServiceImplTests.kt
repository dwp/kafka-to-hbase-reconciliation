package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.*
import org.apache.hadoop.hbase.client.Connection
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import java.sql.ResultSet
import java.sql.Statement

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [ReconciliationServiceImpl::class])
@TestPropertySource(properties = [
	"hbase.zookeeper.quorum=localhost",
	"metadatastore.endpoint=localhost",
	"metadatastore.port=3306",
	"metadatastore.user=dummy_user",
	"metadatastore.password=user_password"

])
class ReconciliationServiceImplTests {

	@SpyBean
	@Autowired
	private lateinit var reconciliationService: ReconciliationService

	@MockBean
	private lateinit var hbaseConnection: Connection

	@MockBean
	private lateinit var metadatastoreConnection: java.sql.Connection


	@Test
	fun contextLoads() {
	}

	// open a connection to metadata store
	// open a connection to HBase
	// handle empty result from metadata store
	@Test
	fun willHandleEmptyResultFromMetadataStore() {
	}

	// limit the number of records returned when querying metadata store
	@Test
	fun limitsTheNumberOfRecordsReturnedFromMetadataStore() {
		val resultSet = mock<ResultSet> {
			on {
				next()
			} doReturnConsecutively listOf(true, true, false)
			on {
				getInt("id")
			} doReturnConsecutively listOf(1, 2)
		}
		val statement = mock<Statement> {
			on {
				executeQuery(any())
			} doReturn resultSet
		}
		given(metadatastoreConnection.createStatement()).willReturn(statement)
		reconciliationService.reconciliation()

		verify(metadatastoreConnection, times(1)).createStatement()
		val captor = argumentCaptor<String>()
		verify(statement, times(1)).executeQuery(captor.capture())
		assert(captor.firstValue.contains("LIMIT"))
	}

	// limit age of messages for reconciliation i.e. older records not returned from metadata store
	@Test
	fun limitsTheAgeOfRecordsReturnedFromMetadataStore() {
		val statement = mock<Statement>()
		given(metadatastoreConnection.createStatement()).willReturn(statement)
		reconciliationService.reconciliation()

		verify(metadatastoreConnection, times(1)).createStatement()
		val captor = argumentCaptor<String>()
		verify(statement, times(1)).executeQuery(captor.capture())
		assert(captor.firstValue.contains("WHERE write_timestamp >"))
	}

	// reconciled records are not returned
	@Test
	fun reconcileRecordsNotReturnedFromMetadataStore() {
		val statement = mock<Statement>()
		given(metadatastoreConnection.createStatement()).willReturn(statement)
		reconciliationService.reconciliation()

		verify(metadatastoreConnection, times(1)).createStatement()
		val captor = argumentCaptor<String>()
		verify(statement, times(1)).executeQuery(captor.capture())
		assert(captor.firstValue.contains("WHERE reconciled_result = false"))
	}
	// validate result when querying metadata store
	// query non-master hbase region
	// batch requests to hbase
	// hbase query response is validated
	// limit max items in hbase query batch
	// run parallel HBase queries
	// if found in Hbase, update metadata store records with reconciled_result=true and reconciled_timestamp=current_timestamp
	// if not found in HBase don't update metadata verifyZeroInteractions
	// metadata store updates are done in batches
	// response from metadata store updates are validated
	// application runs on an interval i.e. it pauses between runs
	// password for metadata store auth is retrieved from secrets manager
	// if auth fails then password is re-retrieved
	// if metadata connection fails it retries connecting
	// if hbase connection fails it retries connecting

}
