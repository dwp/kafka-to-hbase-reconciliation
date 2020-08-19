import io.kotlintest.specs.StringSpec
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.HbaseConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.MetadataStoreConfiguration

class ReconciliationIntegrationTest: StringSpec() {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ReconciliationIntegrationTest::class.toString())
    }

    private lateinit var metadataStoreConfiguration: MetadataStoreConfiguration
    private lateinit var hbaseConfiguration: HbaseConfiguration

    init {
        "Reconciles records that are in metadata store and in hbase" {
            setupMetadataStoreData()
            setupHbaseData()


        }
    }

    private fun setupMetadataStoreData() {
        val connection = metadataStoreConfiguration.metadataStoreConnection()
        val entries = 2
        for (i in 0..entries) {
            connection.prepareStatement("""
                INSERT INTO ${metadataStoreConfiguration.table} (hbase_id, hbase_timestamp, topic_name, write_timestamp, reconciled_result)
                VALUES (123, 1544799662000, topic_name, CURRENT_DATE - INTERVAL 7 DAY, false)
            """.trimIndent())
        }
    }

    private fun setupHbaseData() {

        val connection = hbaseConfiguration.hbaseConnection()
        val columnFamily = "cf".toByteArray()
        val columnQualifier = "record".toByteArray()

        val table = connection.getTable(TableName.valueOf("namespace_table"))

        val key = "1234".toByteArray()
        val body = wellFormedValidPayload()

        val entries = 1
        for (i in 0..entries) {
            table.put(Put(key).apply {
                addColumn(columnFamily, columnQualifier, 1544799662000, body)
            })
        }
    }
}
