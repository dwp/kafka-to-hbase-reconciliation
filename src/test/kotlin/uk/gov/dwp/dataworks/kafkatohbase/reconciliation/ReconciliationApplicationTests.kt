package uk.gov.dwp.dataworks.kafkatohbase.reconciliation

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
class ReconciliationApplicationTests {

	@Test
	fun contextLoads() {
	}

	// open a connection to metadata store
	// open a connection to HBase
	// handle empty result from metadata store
	// limit the number of records returned when querying metadata store
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
