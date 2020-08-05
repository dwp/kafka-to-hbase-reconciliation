package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import org.apache.hadoop.hbase.client.Connection
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService

@Service
class ReconciliationServiceImpl(
    private val hbaseConnection: Connection,
    private val metadatastoreConnection: java.sql.Connection
): ReconciliationService {
    override fun reconciliation() {
        println("$hbaseConnection")
        println("$metadatastoreConnection")
        fetchUnreconciledRecords().forEach {
            if (recordExistsInHbase(it)) {
                reconcileRecord()
            }
        }
    }
    // retrieve records from metadata store
    private fun fetchUnreconciledRecords(): Array<String> {
        return arrayOf("a", "b")
    }

    // check for items in HBase
    private fun recordExistsInHbase(record: String): Boolean {
        return true
    }

    // If found then update metadata store
    private fun reconcileRecord() {

    }
}
