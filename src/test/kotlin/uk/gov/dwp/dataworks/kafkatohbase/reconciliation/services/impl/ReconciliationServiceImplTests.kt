package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
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
//		given(context.hbaseConnection()).willReturn(ConnectionFactory.createConnection())
//		given(context.metadatastoreConnection()).willReturn(DriverManager.getConnection("jdbc:mysql://localhost:3306/"))
//		assertEquals(123, 123)
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
		reconciliationService.reconciliation()

		// I think this will be ditched and the calls to the connections validated instead
		// (at which point the 2 methods below can be reverted to private).
		if (reconciliationService is ReconciliationServiceImpl) {
			verify(reconciliationService as ReconciliationServiceImpl, times(1)).fetchUnreconciledRecords()
			verify(reconciliationService as ReconciliationServiceImpl, times(2)).reconcileRecord()
		}
	}
	// limit age of messages for reconciliation i.e. older records not returned from metadata store
	// reconciled records are not returned
	// validate result when querying metadata store
	// query non-master hbase region
	// batch requests to hbase
	// hbase query response is validated
	// limit max items in hbase query batch
	// run parallel HBase queries
	// if found in Hbase, update metadata store records with reconciled_result=true and reconciled_timestamp=current_timestamp
	// metadata store updates are done in batches
	// response from metadata store updates are validated
	// application runs on an interval i.e. it pauses between runs
	// password for metadata store auth is retrieved from secrets manager
	// if auth fails then password is re-retrieved
	// if metadata connection fails it retries connecting
	// if hbase connection fails it retries connecting

}
