import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.junit.Ignore
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.ReconciliationApplication
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.HBaseConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.MetadataStoreConfiguration


@RunWith(SpringRunner::class)
@SpringBootTest(
    classes = [ReconciliationApplication::class]
)
@ActiveProfiles("DUMMY_SECRETS")
class ReconciliationIntegrationTest {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ReconciliationIntegrationTest::class.toString())
    }

    @Autowired
    lateinit var metadataStoreConfiguration: MetadataStoreConfiguration

    @Autowired
    lateinit var HBaseConfiguration: HBaseConfiguration

    final val hbaseNamespace = "claimant_advances"
    final val hbaseTable = "advanceDetails"
    final val hbaseNamespaceAndTable = "$hbaseNamespace:$hbaseTable"
    final val hbaseTableObject = TableName.valueOf(hbaseNamespaceAndTable)

    final val columnFamily = "cf".toByteArray()
    final val columnQualifier = "record".toByteArray()

    final val kafkaDb = "claimant-advances"
    final val kafkaCollection = "advanceDetails"
    final val kafkaTopic = "$kafkaDb.$kafkaCollection"

    @Test
    fun integrationSpringContextLoads() {
    }

    @Test
    fun testWeCanEmptyHBase() {
        emptyHBaseTable()
    }

    @Test
    fun testWeCanEmptyMetadataStore() {
        emptyMetadataStoreTable()
    }

    @Ignore
    fun testWeCanFillHBase() {
        setupHBaseData(1)
    }

    @Ignore
    fun testWeCanCheckHBase() {
        recordsInHBase()
    }

    @Ignore
    fun testWeCanFillMetastore() {
        setupMetadataStoreData(1)
    }

    @Ignore
    fun testWeCanCheckMetastoreForReconciled() {
        verifyRecordsInMetadataAreReconciled()
    }

    @Ignore
    fun testWeCanCheckMetastore() {
        recordsInMetadataStore()
    }

    @Ignore
    fun givenMatchingRecordsInMetadataStoreAndHBaseWhenStartingReconciliationThenAllRecordsAreReconciled() {

        //given
        emptyHBaseTable()
        emptyMetadataStoreTable()

        val recordsInMetadataStore = recordsInMetadataStore()
        val recordsInHBase = recordsInHBase()

        assertThat(recordsInMetadataStore).isEqualTo(0)
        assertThat(recordsInHBase).isEqualTo(0)

        //when
        val recordsUnderTest = 2
        setupHBaseData(recordsUnderTest)
        setupMetadataStoreData(recordsUnderTest)

        //wait for that to be processed
        do{
            logger.info("Waiting for verified records count to change")
            Thread.sleep(1000)
        } while (verifyRecordsInMetadataAreReconciled() != recordsUnderTest)

        //then
        assertThat(verifyRecordsInMetadataAreReconciled()).isEqualTo(recordsUnderTest)
        assertThat(recordsInMetadataStore()).isEqualTo(recordsUnderTest)
        assertThat(recordsInHBase()).isEqualTo(recordsUnderTest)
    }

    private fun emptyMetadataStoreTable() {
        logger.info("Start emptyMetadataStoreTable")
        metadataStoreConfiguration.metadataStoreConnection().use { connection ->
            with(connection.createStatement()) {
                this.execute(
                    """DELETE FROM ${metadataStoreConfiguration.table};"""
                )
            }
        }
        logger.info("End emptyMetadataStoreTable")
    }

    private fun emptyHBaseTable() {
        logger.info("Start emptyHBaseTable")
        val hbaseAdmin = HBaseConfiguration.hbaseConnection().admin

        if (hbaseAdmin.isTableEnabled(hbaseTableObject)) {
            hbaseAdmin.disableTableAsync(hbaseTableObject)
        }
        do {
            logger.info("emptyHBaseTable: waiting for table to be disabled")
            Thread.sleep(1000)
        } while (hbaseAdmin.isTableEnabled(hbaseTableObject))

        logger.info("emptyHBaseTable: truncating table")
        hbaseAdmin.truncateTable(hbaseTableObject, false)

        if (hbaseAdmin.isTableDisabled(hbaseTableObject)) {
            hbaseAdmin.enableTableAsync(hbaseTableObject)
        }
        do {
            logger.info("emptyHBaseTable: waiting for table to be enabled")
            Thread.sleep(1000)
        } while (hbaseAdmin.isTableDisabled(hbaseTableObject))

        logger.info("End emptyHBaseTable")
    }

    private fun setupHBaseData(entries: Int) {
        logger.info("Start Setup hbase data entries for integration test", "entries" to entries)
        val hbaseAdmin = HBaseConfiguration.hbaseConnection().admin
        hbaseAdmin.enableTable(hbaseTableObject)
        HBaseConfiguration.hbaseConnection().use { connection ->

            with(connection.getTable(hbaseTableObject)) {

                val body = wellFormedValidPayload(hbaseNamespace, hbaseTable)

                for (i in 0..entries) {
                    val key = i.toString().toByteArray()
                    this.put(Put(key).apply {
                        addColumn(columnFamily, columnQualifier, 1544799662000, body)
                    })
                }
            }
        }
        logger.info("Done Setup hbase data entries for integration test", "entries" to entries)
    }

    private fun recordsInHBase(): Int {
        logger.info("Start recordsInHBase")
        var found = 0
        HBaseConfiguration.hbaseConnection().use { connection ->
            with(connection.getTable(hbaseTableObject)) {
                val scanner = getScanner(Scan())
                do {
                    val result = scanner.next()
                    if (result != null) {
                        found++
                        val latestId = result.row.toString()
                        logger.info("Found hbase row", "row_index" to "$found", "row_key" to "$latestId")
                    }
                } while (result != null)
            }
        }
        logger.info("End recordsInHBase", "records_found" to found)
        return found
    }

    fun printableKey(key: ByteArray) =
        if (key.size > 4) {
            val hash = key.slice(IntRange(0, 3))
            val hex = hash.map { String.format("\\x%02X", it) }.joinToString("")
            val renderable = key.slice(IntRange(4, key.size - 1)).map { it.toChar() }.joinToString("")
            "${hex}${renderable}"
        } else {
            String(key)
        }

    private fun setupMetadataStoreData(entries: Int) {
        logger.info("Start Setup metadata store data entries for integration test", "entries" to entries)
        metadataStoreConfiguration.metadataStoreConnection().use { connection ->
            for (index in 0..entries) {
                val key = index.toString() //check symmetry with hbase key -> val key = i.toString().toByteArray()
                val statement = connection.createStatement()
                statement.executeQuery(
                    """
                    INSERT INTO ${metadataStoreConfiguration.table} (hbase_id, hbase_timestamp, topic_name, write_timestamp, reconciled_result)
                    VALUES ($key, 1544799662000, $kafkaTopic, CURRENT_DATE - INTERVAL 7 DAY, false)
                """.trimIndent()
                )
                logger.info(
                    "Added metadata store data entries for integration test",
                    "index" to "$index", "hbase_id" to key, "topic_name" to kafkaTopic
                )
            }
        }
        logger.info("End Setup metadata store data entries for integration test", "entries" to entries)
    }

    private fun verifyRecordsInMetadataAreReconciled(): Int {
        metadataStoreConfiguration.metadataStoreConnection().use { connection ->
            with(connection.createStatement()) {
                val rs = this.executeQuery(
                    """
                SELECT COUNT(*) FROM ${metadataStoreConfiguration.table} WHERE reconciled_result=true
            """.trimIndent()
                )
                rs.next()
                return rs.getInt(1)
            }
        }
    }

    private fun recordsInMetadataStore(): Int {
        metadataStoreConfiguration.metadataStoreConnection().use { connection ->
            with(connection.createStatement()) {
                val rs = this.executeQuery(
                    """SELECT COUNT(*) FROM ${metadataStoreConfiguration.table} """.trimIndent()
                )
                rs.next()
                return rs.getInt(1)
            }
        }
    }

}
