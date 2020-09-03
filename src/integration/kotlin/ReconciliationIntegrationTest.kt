import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes.toBytes
import org.junit.Ignore
import org.junit.jupiter.api.Test
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
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import java.lang.UnsupportedOperationException


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

//    @Autowired
//    lateinit var service: ReconciliationService

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
        verifyRecordsInMetadataAreReconciled(1)
    }

    @Ignore
    fun testWeCanCheckMetastore() {
        recordsInHBase()
    }

//    @Ignore
//    fun givenNoRecordsInMetadataStoreAndHBaseWhenStartingReconciliationThenNoRecordsAreReconciled() {
//
//        val recordsInMetadataStore = recordsInMetadataStore()
//        val recordsInHBase = recordsInHBase()
//
//        assert(recordsInMetadataStore == 0)
//        assert(recordsInHBase == 0)
//
//        service.startReconciliation()
//
//        assert(recordsInMetadataStore == 0)
//        assert(recordsInHBase == 0)
//    }

//    @Ignore
//    fun givenRecordsToBeReconciledInMetadataStoreWhenRecordsExistInHBaseThenTheRecordsAreReconciled() {
//
//        createMetadataStoreTable()
//        createHBaseTable()
//
//        setupHBaseData(5)
//        setupMetadataStoreData(5)
//
//        service.startReconciliation()
//
//        val haveBeenReconciled = verifyRecordsInMetadataAreReconciled(5)
//
//        assert(haveBeenReconciled)
//    }

//    @Ignore
//    fun givenRecordsToBeReconciledInMetadataStoreWhenRecordsExistInHBasePlusExtraThenTheRecordsAreReconciledThatOnlyExistInMetadataStore() {
//
//        createMetadataStoreTable()
//
//        setupHBaseData(10)
//        setupMetadataStoreData(5)
//
//        service.startReconciliation()
//
//        val haveBeenReconciled = verifyRecordsInMetadataAreReconciled(5)
//
//        assert(haveBeenReconciled)
//    }

//    @Ignore
//    fun givenFiveRecordsToBeReconciledInMetadataStoreAndTwoInHBaseWhenRequestingToReconcileThenOnlyTwoAreReconciled() {
//
//        createMetadataStoreTable()
//        createHBaseTable()
//
//        setupHBaseData(2)
//        setupMetadataStoreData(5)
//
//        service.startReconciliation()
//
//        val haveBeenReconciled = verifyRecordsInMetadataAreReconciled(2)
//
//        assert(haveBeenReconciled)
//    }

//    private fun createMetadataStoreTable() {
//        metadataStoreConfiguration.metadataStoreConnection().use { connection ->
//            with(connection.createStatement()) {
//                this.execute(
//                    """
//                     CREATE TABLE IF NOT EXISTS `${metadataStoreConfiguration.table}` (
//                    `id` INT NOT NULL AUTO_INCREMENT,
//                    `hbase_id` VARCHAR(2048) NULL,
//                    `hbase_timestamp` DATETIME NULL,
//                    `write_timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
//                    `correlation_id` VARCHAR(1024) NULL,
//                    `topic_name` VARCHAR(1024) NULL,
//                    `kafka_partition` INT NULL,
//                    `kafka_offset` INT NULL,
//                    `reconciled_result` TINYINT(1) NOT NULL DEFAULT 0,
//                    `reconciled_timestamp` DATETIME NULL,
//                    PRIMARY KEY (`id`),
//                    INDEX (hbase_id,hbase_timestamp),
//                    INDEX (write_timestamp),
//                    INDEX (reconciled_result)
//                );
//                """
//                )
//            }
//        }
//    }

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

//    private fun createHBaseTable() {
//        logger.info("Start createHBaseTable")
//        val table = HTableDescriptor(hbaseTableObject)
//        val family = HColumnDescriptor(toBytes("cf"))
//        val qualifier = HColumnDescriptor(toBytes("record"))
//        table.addFamily(family)
//        table.addFamily(qualifier)
//        val hbaseAdmin = HBaseConfiguration.hbaseConnection().admin
//        hbaseAdmin.createTable(table)
//        logger.info("End createHBaseTable")
//    }

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
        }
        else {
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
                logger.info("Added metadata store data entries for integration test",
                    "index" to "$index", "hbase_id" to key, "topic_name" to kafkaTopic)
            }
        }
        logger.info("End Setup metadata store data entries for integration test", "entries" to entries)
    }

    private fun verifyRecordsInMetadataAreReconciled(shouldBeReconciledCount: Int): Boolean {
        metadataStoreConfiguration.metadataStoreConnection().use { connection ->
            with(connection.createStatement()) {
                return this.execute(
                    """
                SELECT CASE WHEN COUNT(*) = $shouldBeReconciledCount THEN TRUE ELSE FALSE END;
                FROM ${metadataStoreConfiguration.table}
                WHERE reconciled_result=true
            """.trimIndent()
                )
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
