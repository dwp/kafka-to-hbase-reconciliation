package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.*
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Table
import org.junit.Ignore
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.TextUtils
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import java.sql.ResultSet
import java.sql.Statement

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [ReconciliationServiceImpl::class, TextUtils::class])
@TestPropertySource(properties = [
	"hbase.zookeeper.quorum=localhost",
	"hbase.table.pattern=^\\\\w+\\\\.([-\\\\w]+)\\.([-\\\\w]+)$",
	"metadatastore.endpoint=localhost",
	"metadatastore.port=3306",
	"metadatastore.user=dummy_user",
	"metadatastore.password=user_password"
])
class ReconciliationServiceImplTests {

	@SpyBean
	@Autowired
	private lateinit var reconciliationService: ReconciliationService

	@Autowired
	private lateinit var textUtils: TextUtils

	@MockBean
	private lateinit var hbaseConnection: Connection

	@MockBean
	private lateinit var metadatastoreConnection: java.sql.Connection


	@Test
	fun contextLoads() {
	}

	// handle empty result from metadata store
	@Test
	fun willHandleEmptyResultFromMetadataStore() {
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
		given(metadatastoreConnection.createStatement()).willReturn(statement)
		reconciliationService.reconciliation()

		verify(metadatastoreConnection, times(1)).createStatement()
		val captor = argumentCaptor<String>()
		verify(statement, times(1)).executeQuery(any())
	}

	// limit the number of records returned when querying metadata store
	@Test
	fun limitsTheNumberOfRecordsReturnedFromMetadataStore() {
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
		val pattern = Regex("""(WHERE|AND) write_timestamp >""")
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
		given(metadatastoreConnection.createStatement()).willReturn(statement)
		reconciliationService.reconciliation()

		verify(metadatastoreConnection, times(1)).createStatement()
		val captor = argumentCaptor<String>()
		verify(statement, times(1)).executeQuery(captor.capture())
		assert(captor.firstValue.contains(pattern))
	}

	// reconciled records are not returned
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
		given(metadatastoreConnection.createStatement()).willReturn(statement)
		reconciliationService.reconciliation()

		verify(metadatastoreConnection, times(1)).createStatement()
		val captor = argumentCaptor<String>()
		verify(statement, times(1)).executeQuery(captor.capture())
		assert(captor.firstValue.contains(pattern))
	}
	// validate result when querying metadata store
	// query non-master hbase region
	// batch requests to hbase
	// hbase query response is validated
	// limit max items in hbase query batch
	// run parallel HBase queries

	// only update metadata store records if found in Hbase, set reconciled_result=true and reconciled_timestamp=current_timestamp
	@Test
	fun updatesMetadataStoreOnlyWhenFoundInHbase() {
		// Set up two records to be reconciled, only one will exist in HBase
		val resultSet = mock<ResultSet> {
			on {
				next()
			} doReturnConsecutively listOf(true, true, false)
			on {
				getString("id")
			} doReturnConsecutively listOf("1", "2")
			on {
				getString("hbase_id")
			} doReturnConsecutively listOf("1", "2")
			on {
				getString("hbase_timestamp")
			} doReturnConsecutively listOf("1", "2")
			on {
				getString("write_timestamp")
			} doReturnConsecutively listOf("1", "2")
			on {
				getString("correlation_id")
			} doReturnConsecutively listOf("1", "2")
			on {
				getString("topic_name")
			} doReturnConsecutively listOf("db.database.collection", "db.database.collection")
			on {
				getString("kafka_partition")
			} doReturnConsecutively listOf("1", "2")
			on {
				getString("kafka_offest")
			} doReturnConsecutively listOf("1", "2")
			on {
				getString("reconciled_result")
			} doReturnConsecutively listOf("1", "2")
			on {
				getString("reconciled_timestamp")
			} doReturnConsecutively listOf("1", "2")
		}
		val statement = mock<Statement> {
			on {
				executeQuery(any())
			} doReturn resultSet
		}
		given(metadatastoreConnection.createStatement()).willReturn(statement)
		val table = mock<Table> {
			on {
				exists(any())
			} doReturnConsecutively listOf(true, false)
		}
		given(hbaseConnection.getTable(any())).willReturn(table)

		reconciliationService.reconciliation()
		verify(metadatastoreConnection, times(2)).createStatement()
		val captor = argumentCaptor<String>()
		verify(statement, times(2)).executeQuery(captor.capture())

		// Check metadata store queried once
		assert(captor.firstValue.contains("SELECT"))
		// Check data is only reconciled when found in HBase
		assert(captor.secondValue.contains("UPDATE"))
		assert(captor.secondValue.contains("reconciled_result=true"))
		assert(captor.secondValue.contains("reconciled_timestamp=current_timestamp"))
	}

	// metadata store updates are done in batches
	// response from metadata store updates are validated
	// application runs on an interval i.e. it pauses between runs
	// password for metadata store auth is retrieved from secrets manager
	// if auth fails then password is re-retrieved
	// if metadata connection fails it retries connecting
	// if hbase connection fails it retries connecting

}
