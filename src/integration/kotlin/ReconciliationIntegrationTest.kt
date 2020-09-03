import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.Put
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
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.HbaseConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.MetadataStoreConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService


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
    lateinit var hbaseConfiguration: HbaseConfiguration

    @Autowired
    lateinit var service: ReconciliationService

    val hbaseNamespace = "claimant_advances"
    val hbaseTable = "advanceDetails"
    val hbaseNamespaceAndTable = "$hbaseNamespace:$hbaseTable"
    val hbaseTableObject = TableName.valueOf(hbaseNamespaceAndTable)

    val columnFamily = "cf".toByteArray()
    val columnQualifier = "record".toByteArray()

    @Test
    fun integrationSpringContextLoads() {
    }

    @Test
    fun testWeCanEmptyHBase() {
        emptyHbaseTable()
    }

    @Test
    fun testWeCanEmptyMetadataStore() {
        emptyMetadataStoreTable()
    }

    @Ignore
    fun givenNoRecordsInMetadataStoreAndHbaseWhenStartingReconciliationThenNoRecordsAreReconciled() {

        val recordsInMetadataStore = recordsInMetadataStore()
        val recordsInHbase = recordsInHbase()

        assert(recordsInMetadataStore == 0)
        assert(recordsInHbase == 0)

        service.startReconciliation()

        assert(recordsInMetadataStore == 0)
        assert(recordsInHbase == 0)
    }

    @Ignore
    fun givenRecordsToBeReconciledInMetadataStoreWhenRecordsExistInHbaseThenTheRecordsAreReconciled() {

        createMetadataStoreTable()
        createHbaseTable()

        setupHbaseData(5)
        setupMetadataStoreData(5)

        service.startReconciliation()

        val haveBeenReconciled = verifyRecordsInMetadataAreReconciled(5)

        assert(haveBeenReconciled)
    }

    @Ignore
    fun givenRecordsToBeReconciledInMetadataStoreWhenRecordsExistInHbasePlusExtraThenTheRecordsAreReconciledThatOnlyExistInMetadataStore() {

        createMetadataStoreTable()

        setupHbaseData(10)
        setupMetadataStoreData(5)

        service.startReconciliation()

        val haveBeenReconciled = verifyRecordsInMetadataAreReconciled(5)

        assert(haveBeenReconciled)
    }

    @Ignore
    fun givenFiveRecordsToBeReconciledInMetadataStoreAndTwoInHbaseWhenRequestingToReconcileThenOnlyTwoAreReconciled() {

        createMetadataStoreTable()
        createHbaseTable()

        setupHbaseData(2)
        setupMetadataStoreData(5)

        service.startReconciliation()

        val haveBeenReconciled = verifyRecordsInMetadataAreReconciled(2)

        assert(haveBeenReconciled)
    }

    private fun createMetadataStoreTable() {
        metadataStoreConfiguration.metadataStoreConnection().use { connection ->
            with(connection.createStatement()) {
                this.execute(
                    """
                     CREATE TABLE IF NOT EXISTS `ucfs` (
                    `id` INT NOT NULL AUTO_INCREMENT,
                    `hbase_id` VARCHAR(2048) NULL,
                    `hbase_timestamp` DATETIME NULL,
                    `write_timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
                    `correlation_id` VARCHAR(1024) NULL,
                    `topic_name` VARCHAR(1024) NULL,
                    `kafka_partition` INT NULL,
                    `kafka_offset` INT NULL,
                    `reconciled_result` TINYINT(1) NOT NULL DEFAULT 0,
                    `reconciled_timestamp` DATETIME NULL,
                    PRIMARY KEY (`id`),
                    INDEX (hbase_id,hbase_timestamp),
                    INDEX (write_timestamp),
                    INDEX (reconciled_result)
                );
                """
                )
            }
        }
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

    private fun createHbaseTable() {
        logger.info("Start createHbaseTable")
        val admin = HBaseAdmin(hbaseConfiguration.hbaseConfiguration())

        val table = HTableDescriptor(hbaseTableObject)

        val family = HColumnDescriptor(toBytes("cf"))

        val qualifier = HColumnDescriptor(toBytes("record"))

        table.addFamily(family)
        table.addFamily(qualifier)

        admin.createTable(table)
        logger.info("End createHbaseTable")
    }

    private fun emptyHbaseTable() {
        logger.info("Start emptyHbaseTable")
        val admin = HBaseAdmin(hbaseConfiguration.hbaseConfiguration())
        admin.truncateTable(hbaseTableObject, true)
        logger.info("End emptyHbaseTable")
    }

    private fun setupHbaseData(entries: Int) {
        logger.info("Start Setup hbase data entries for integration test", "entries" to entries)
        hbaseConfiguration.hbaseConnection().use { connection ->

            with(connection.getTable(hbaseTableObject)) {

                val body = wellFormedValidPayload()

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

    private fun setupMetadataStoreData(entries: Int) {
        logger.info("Start Setup metadata store data entries for integration test", "entries" to entries)
        metadataStoreConfiguration.metadataStoreConnection().use { connection ->
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

    private fun recordsInHbase(): Int {
        val connection = metadataStoreConfiguration.metadataStoreConnection()
        val statement = connection.createStatement()
        val rs = statement.executeQuery(
            """SELECT COUNT(*) FROM ${metadataStoreConfiguration.table} """.trimIndent()
        )
        rs.next()
        return rs.getInt(1)
    }
}
