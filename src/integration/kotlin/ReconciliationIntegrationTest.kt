import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.ReconciliationApplication
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.HbaseConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.MetadataStoreConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService

@RunWith(SpringRunner::class)
@SpringBootTest(
    classes = [ReconciliationApplication::class]
)
@TestPropertySource(
    properties = [
        "hbase.zookeeper.quorum=localhost",
        "hbase.table.pattern=^\\\\w+\\\\.([-\\\\w]+)\\.([-\\\\w]+)$",
        "metadatastore.endpoint=localhost",
        "metadatastore.port=3306",
        "metadatastore.user=reconciliationwriter",
        "metadatastore.password=password",
        "metadatastore.databasename=metadatastore",
        "metadatastore.table=ucfs"]
)
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

    @Test
    fun givenNoRecordsInMetadataStoreAndHbaseWhenStartingReconciliationThenNoRecordsAreReconciled() {

        val recordsInMetadataStore = recordsInMetadataStore()
        val recordsInHbase = recordsInHbase()
        
        assert(recordsInMetadataStore == 0)
        assert(recordsInHbase == 0)

        service.startReconciliation()

        assert(recordsInMetadataStore == 0)
        assert(recordsInHbase == 0)
    }

    @Test
    fun givenRecordsToBeReconciledInMetadataStoreWhenRecordsExistExactlyInHbaseThenTheRecordsAreReconciled() {

        createMetadataStoreTable()

        setupHbaseData(5)
        setupMetadataStoreData(5)

        service.startReconciliation()

        val haveBeenReconciled = verifyRecordsInMetadataAreReconciled(5)

        assert(haveBeenReconciled)
    }

    @Test
    fun givenRecordsToBeReconciledInMetadataStoreWhenRecordsExistInHbasePlusExtraThenTheRecordsAreReconciledThatOnlyExistInMetadataStore() {

        createMetadataStoreTable()

        setupHbaseData(10)
        setupMetadataStoreData(5)

        service.startReconciliation()

        val haveBeenReconciled = verifyRecordsInMetadataAreReconciled(5)

        assert(haveBeenReconciled)
    }

    @Test
    fun givenFiveRecordsToBeReconciledInMetadataStoreAndTwoInHbaseWhenRequestingToReconcileThenOnlyTwoAreReconciled() {

        createMetadataStoreTable()

        setupHbaseData(2)
        setupMetadataStoreData(5)

        service.startReconciliation()

        val haveBeenReconciled = verifyRecordsInMetadataAreReconciled(2)

        assert(haveBeenReconciled)
    }

    private fun createMetadataStoreTable() {

        val connection = metadataStoreConfiguration.metadataStoreConnection()

        val statement = connection.createStatement()
        statement.execute(
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
        logger.info("Setup hbase data entries for integration test", "entries" to entries)
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
        logger.info("Setup metadata store data entries for integration test", "entries" to entries)
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

    private fun recordsInMetadataStore(): Int {
        val connection = metadataStoreConfiguration.metadataStoreConnection()
        val statement = connection.createStatement()
        val rs = statement.executeQuery(
            """
                SELECT COUNT(*)
                FROM ${metadataStoreConfiguration.table}
            """.trimIndent()
        )
        rs.next()
        return rs.getInt(1)
    }

    private fun recordsInHbase(): Int {
        val connection = metadataStoreConfiguration.metadataStoreConnection()
        val statement = connection.createStatement()
        val rs = statement.executeQuery(
            """
                SELECT COUNT(*)
                FROM ${metadataStoreConfiguration.table}
            """.trimIndent()
        )
        rs.next()
        return rs.getInt(1)
    }
}
