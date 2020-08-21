import io.kotlintest.specs.StringSpec
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.HbaseConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.MetadataStoreConfiguration

class ReconciliationIntegrationTest : StringSpec() {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ReconciliationIntegrationTest::class.toString())
    }

    private lateinit var metadataStoreConfiguration: MetadataStoreConfiguration
    private lateinit var hbaseConfiguration: HbaseConfiguration

    init {
        "Reconciles records that are in metadata store and in hbase" {
            setupHbaseData(5)
            setupMetadataStoreData(5)

            val haveBeenReconciled = verifyRecordsInMetadataAreReconciled(5)

            assert(haveBeenReconciled)
        }
    }

    private fun setupHbaseData(entries: Int) {

        val connection = hbaseConfiguration.hbaseConnection()
        val columnFamily = "cf".toByteArray()
        val columnQualifier = "record".toByteArray()

        val table = connection.getTable(TableName.valueOf("namespace_table"))

        val body = wellFormedValidPayload()

        for (i in 0..entries) {
            val key = i.toString().toByteArray()
            table.put(Put(key).apply {
                addColumn(columnFamily, columnQualifier, 1544799662000, body)
            })
        }
    }

    private fun setupMetadataStoreData(entries: Int) {
        val connection = metadataStoreConfiguration.metadataStoreConnection()
        for (i in 0..entries) {
            val key = i.toString()
            val statement = connection.createStatement()
            statement.executeQuery(
                """
                    INSERT INTO ${metadataStoreConfiguration.table} (hbase_id, hbase_timestamp, topic_name, write_timestamp, reconciled_result)
                    VALUES ($key, 1544799662000, topic_name, CURRENT_DATE - INTERVAL 7 DAY, false)
                """.trimIndent()
            )
        }
    }

    private fun verifyRecordsInMetadataAreReconciled(shouldBeReconciledCount: Int): Boolean {
        val connection = metadataStoreConfiguration.metadataStoreConnection()
        val statement = connection.createStatement()
        return statement.execute(
            """
                SELECT CASE WHEN COUNT(*) = $shouldBeReconciledCount THEN TRUE ELSE FALSE END;
                FROM ${metadataStoreConfiguration.table}
                WHERE reconciled_result=true
            """.trimIndent()
        )
    }
}
