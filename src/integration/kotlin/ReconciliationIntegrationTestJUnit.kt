import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.junit.Test
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit.jupiter.SpringExtension
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.HbaseConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.MetadataStoreConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.SecretsManagerConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl.ReconciliationServiceImpl

@ExtendWith(SpringExtension::class)
@SpringBootTest(
    classes = [HbaseConfiguration::class, MetadataStoreConfiguration::class, SecretsManagerConfiguration::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@TestPropertySource(locations = ["classpath:application.yml"])
@EnableAutoConfiguration
class ReconciliationIntegrationTestJUnit {

    @Autowired
    private lateinit var metadataStoreConfiguration: MetadataStoreConfiguration

    @Autowired
    private lateinit var hbaseConfiguration: HbaseConfiguration

    @Autowired
    private lateinit var reconciliationService: ReconciliationServiceImpl

    @Test
    fun applicationIsConfiguredCorrectly() {
        assertNotNull(metadataStoreConfiguration.table)
        assertNotNull(metadataStoreConfiguration.databaseName)
        assertNotNull(metadataStoreConfiguration.endpoint)
        assertNotNull(metadataStoreConfiguration.password)
        assertNotNull(metadataStoreConfiguration.passwordSecretName)
        assertNotNull(metadataStoreConfiguration.port)
        assertNotNull(metadataStoreConfiguration.queryLimit)
        assertNotNull(metadataStoreConfiguration.table)

        assertNotNull(hbaseConfiguration.zookeeperParent)
        assertNotNull(hbaseConfiguration.zookeeperPort)
        assertNotNull(hbaseConfiguration.zookeeperQuorum)
        assertNotNull(hbaseConfiguration.rpcTimeoutMilliseconds)
        assertNotNull(hbaseConfiguration.operationTimeoutMilliseconds)
        assertNotNull(hbaseConfiguration.pauseMilliseconds)
        assertNotNull(hbaseConfiguration.qualifiedTablePattern)
    }

    @Test
    fun test() {

        setupHbaseData(5)
        setupMetadataStoreData(5)

        reconciliationService.startReconciliation()

        val haveBeenReconciled = verifyRecordsInMetadataAreReconciled(5)

        assert(haveBeenReconciled)
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
        return statement.executeQuery(
            """
                SELECT COUNT(*)
                FROM ${metadataStoreConfiguration.table}
                WHERE reconciled_result=true
            """.trimIndent()
        ).getInt(1) == shouldBeReconciledCount
    }
}
