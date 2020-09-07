import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.assertj.core.api.Assertions.assertThat
import org.junit.Ignore
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
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
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ReconciliationIntegrationTest {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ReconciliationIntegrationTest::class.toString())
    }

    @Autowired
    lateinit var metadataStoreConfiguration: MetadataStoreConfiguration

    @Autowired
    lateinit var hbaseConfiguration: HBaseConfiguration

    var metadataStoreConnection: java.sql.Connection? = null
    var metadataTable = "NOT_SET"
    var hbaseConnection: org.apache.hadoop.hbase.client.Connection? = null

    final val hbaseNamespace = "claimant_advances"
    final val hbaseTable = "advanceDetails"
    final val hbaseNamespaceAndTable = "$hbaseNamespace:$hbaseTable"
    final val hbaseTableObject = TableName.valueOf(hbaseNamespaceAndTable)

    final val columnFamily = "cf".toByteArray()
    final val columnQualifier = "record".toByteArray()

    final val kafkaDb = "claimant-advances"
    final val kafkaCollection = "advanceDetails"
    final val kafkaTopic = "$kafkaDb.$kafkaCollection"

    @BeforeEach
    fun setup() {
        if (metadataStoreConnection == null) {
            logger.info("Setup metadataStoreConnection")
            //metadataStoreConfiguration = MetadataStoreConfiguration()
            metadataStoreConnection = metadataStoreConfiguration!!.metadataStoreConnection()
            metadataTable = metadataStoreConfiguration!!.table!!
        } else {
            logger.info("Already done metadataStoreConnection")
        }
        if (hbaseConnection == null) {
            logger.info("Setup hbaseConnection")
            //hbaseConfiguration = HBaseConfiguration()
            hbaseConnection = hbaseConfiguration!!.hbaseConnection()
        } else {
            logger.info("Already done hbaseConnection")
        }
    }

    @Test
    fun testThatIntegrationSpringContextLoads() {
    }

    c
    fun testWeCanEmptyHBase() {
        try {
            emptyHBaseTable()
        } catch (ex: Exception) {
            logger.error("Exception in testWeCanEmptyHBase", ex)
            throw ex
        }
    }

    c
    fun testWeCanCheckHBase() {
        try {
            recordsInHBase()
        } catch (ex: Exception) {
            logger.error("Exception in testWeCanCheckHBase", ex)
            throw ex
        }
    }

    @Test
    fun testWeCanEmptyMetadataStore() {
        try {
            emptyMetadataStoreTable()
        } catch (ex: Exception) {
            logger.error("Exception in testWeCanEmptyMetadataStore", ex)
            throw ex
        }
    }

    @Test
    fun testWeCanFillHBase() {
        try {
            setupHBaseData(0, 0)
        } catch (ex: Exception) {
            logger.error("Exception in testWeCanFillHBase", ex)
            throw ex
        }
    }

    @Ignore
    fun testWeCanFillMetastore() {
        try {
            setupMetadataStoreData(0, 0)
        } catch (ex: Exception) {
            logger.error("Exception in testWeCanFillMetastore", ex)
            throw ex
        }
    }

    @Test
    fun testWeCanCheckMetastoreForReconciled() {
        try {
            reconciledRecordsInMetadataStore()
        } catch (ex: Exception) {
            logger.error("Exception in testWeCanCheckMetastoreForReconciled", ex)
            throw ex
        }
    }

    @Test
    fun testWeCanCheckMetastore() {
        try {
            allRecordsInMetadataStore()
        } catch (ex: Exception) {
            logger.error("Exception in testWeCanCheckMetastore", ex)
            throw ex
        }
    }

    @Test
    fun testThatMatchingRecordsAreReconciledAndMismatchesAreNot() {
        try {
            //given
            emptyHBaseTable()
            emptyMetadataStoreTable()

            val recordsInMetadataStore = allRecordsInMetadataStore()
            val recordsInHBase = recordsInHBase()

            assertThat(recordsInMetadataStore).isEqualTo(0)
            assertThat(recordsInHBase).isEqualTo(0)

            //when record 1 is hbase only, 2,3,4 in both, 5 metastore only
            setupHBaseData(1, 4)
            setupMetadataStoreData(2, 5)

            //wait for that to be processed
            do {
                logger.info("Waiting for verified records count to change")
                Thread.sleep(1000)
            } while (reconciledRecordsInMetadataStore() < 3)

            //then
            assertThat(reconciledRecordsInMetadataStore()).isEqualTo(3)
            assertThat(allRecordsInMetadataStore()).isEqualTo(4)
            assertThat(recordsInHBase()).isEqualTo(4)
        } catch (ex: Exception) {
            logger.error("Exception in test", ex)
            throw ex
        }
    }

    private fun emptyMetadataStoreTable() {
        logger.info("Start emptyMetadataStoreTable")
        metadataStoreConnection!!.use { connection ->
            val statement = connection.createStatement()
            statement.execute(
                """DELETE FROM $metadataTable;"""
            )
        }
        logger.info("End emptyMetadataStoreTable")
    }

    private fun emptyHBaseTable() {
        logger.info("Start emptyHBaseTable")
        val hbaseAdmin = hbaseConnection!!.admin

        disableHBaseTable(hbaseAdmin)

        logger.info("emptyHBaseTable: truncating table")
        hbaseAdmin.truncateTable(hbaseTableObject, false)

        enableHBaseTable(hbaseAdmin)

        logger.info("End emptyHBaseTable")
    }

    private fun enableHBaseTable(hbaseAdmin: Admin) {
        logger.info("Start enableHBaseTable")
        if (hbaseAdmin.isTableDisabled(hbaseTableObject)) {
            hbaseAdmin.enableTableAsync(hbaseTableObject)
        }
        do {
            logger.info("emptyHBaseTable: waiting for table to be enabled")
            Thread.sleep(1000)
        } while (hbaseAdmin.isTableDisabled(hbaseTableObject))
        logger.info("End enableHBaseTable")
    }

    private fun disableHBaseTable(hbaseAdmin: Admin) {
        logger.info("Start disableHBaseTable")
        if (hbaseAdmin.isTableEnabled(hbaseTableObject)) {
            hbaseAdmin.disableTableAsync(hbaseTableObject)
        }
        do {
            logger.info("emptyHBaseTable: waiting for table to be disabled")
            Thread.sleep(1000)
        } while (hbaseAdmin.isTableEnabled(hbaseTableObject))
        logger.info("End disableHBaseTable")
    }


    private fun setupHBaseData(startIndex: Int, endIndex: Int) {
        logger.info("Start Setup hbase data entries for integration test", "startIndex" to "$startIndex", "endIndex" to "$endIndex")
        enableHBaseTable(hbaseConnection!!.admin)
        hbaseConnection!!.use { connection ->
            with(connection.getTable(hbaseTableObject)) {

                val body = wellFormedValidPayload(hbaseNamespace, hbaseTable)

                for (index in startIndex..endIndex) {
                    val key = index.toString().toByteArray()
                    this.put(Put(key).apply {
                        addColumn(columnFamily, columnQualifier, 1544799662000, body)
                    })
                    logger.info(
                        "Added hbase entries for integration test", "index" to "$index", "key" to "$key",
                        "hbase_id" to key, "topic_name" to kafkaTopic
                    )
                }
            }
        }
        logger.info("Done Setup hbase data entries for integration test", "startIndex" to "$startIndex", "endIndex" to "$endIndex")
    }

    private fun recordsInHBase(): Int {
        logger.info("Start recordsInHBase")
        enableHBaseTable(hbaseConnection!!.admin)
        var found = 0
        hbaseConnection!!.use { connection ->
            with(connection.getTable(hbaseTableObject)) {
                val scanner = getScanner(Scan())
                do {
                    val result = scanner.next()
                    if (result != null) {
                        found++
                        val latestId = result.row.toString()
                        logger.info("Found hbase row", "row_index" to "$found", "row_key" to latestId)
                    }
                } while (result != null)
            }
        }
        logger.info("End recordsInHBase", "records_found" to found)
        return found
    }

//    fun printableKey(key: ByteArray) =
//        if (key.size > 4) {
//            val hash = key.slice(IntRange(0, 3))
//            val hex = hash.map { String.format("\\x%02X", it) }.joinToString("")
//            val renderable = key.slice(IntRange(4, key.size - 1)).map { it.toChar() }.joinToString("")
//            "${hex}${renderable}"
//        } else {
//            String(key)
//        }

    private fun setupMetadataStoreData(startIndex: Int, endIndex: Int) {
        logger.info("Start Setup metadata store data entries for integration test", "startIndex" to "$startIndex", "endIndex" to "$endIndex")
        metadataStoreConnection!!.use { connection ->
            for (index in startIndex..endIndex) {
                val key = index.toString()
                val statement = connection.createStatement()
                val result = statement.executeQuery(
                    """
                    INSERT INTO $metadataTable (hbase_id, hbase_timestamp, topic_name, write_timestamp, reconciled_result)
                    VALUES ($key, 1544799662000, $kafkaTopic, CURRENT_DATE - INTERVAL 7 DAY, false)
                """.trimIndent()
                )
                logger.info(
                    "Added metadata store data entries for integration test",
                    "result_row_inserted" to "${result.rowInserted()}",
                    "index" to "$index", "hbase_id" to key, "topic_name" to kafkaTopic
                )
            }
        }
        logger.info("End Setup metadata store data entries for integration test", "startIndex" to "$startIndex", "endIndex" to "$endIndex")
    }

    private fun reconciledRecordsInMetadataStore(): Int {
        metadataStoreConnection!!.use { connection ->
            with(connection.createStatement()) {
                val rs = this.executeQuery(
                    """
                SELECT COUNT(*) FROM $metadataTable WHERE reconciled_result=true
            """.trimIndent()
                )
                rs.next()
                return rs.getInt(1)
            }
        }
    }

    private fun allRecordsInMetadataStore(): Int {
        metadataStoreConnection!!.use { connection ->
            with(connection.createStatement()) {
                val rs = this.executeQuery(
                    """SELECT COUNT(*) FROM $metadataTable """.trimIndent()
                )
                rs.next()
                return rs.getInt(1)
            }
        }
    }

}
