package uk.gov.dwp.dataworks.kafkatohbase.reconciliation

import org.apache.hadoop.hbase.client.Connection
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ReconciliationApplication(
    private val hbaseConnection: Connection,
    private val metadatastoreConnection: java.sql.Connection
) : CommandLineRunner {
    override fun run(vararg args: String?) {
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

fun main(args: Array<String>) {
    runApplication<ReconciliationApplication>(*args)
}
