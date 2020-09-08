import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.ReconciliationApplication
import utility.MessageParser
import java.sql.Connection
import java.sql.Timestamp
import java.util.*

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [ReconciliationApplication::class])
@ActiveProfiles("DUMMY_SECRETS")
class ReconciliationIntegrationTest {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ReconciliationIntegrationTest::class.java)
    }

    @Autowired
    lateinit var metadatastoreConnection: Connection

    @Autowired
    lateinit var hbaseConnection: org.apache.hadoop.hbase.client.Connection

    private val columnFamily = "cf".toByteArray()
    private val columnQualifier = "record".toByteArray()
    private val kafkaDb = "claimant-advances"
    private val kafkaCollection = "advanceDetails"
    private val kafkaTopic = "$kafkaDb.$kafkaCollection"

    @Test
    fun testThatMatchingRecordsAreReconciledAndMismatchesAreNot() {
        try {
            //given
            emptyHBaseTable()
            emptyMetadataStoreTable()

            val recordsInMetadataStore = allRecordCount()
            val recordsInHBase = recordsInHBase(qualifiedHbaseTableName)

            assertThat(recordsInMetadataStore).isEqualTo(0)
            assertThat(recordsInHBase).isEqualTo(0)

            //when record 1 is hbase only, 2,3,4 in both, 5 metastore only
            setupHBaseData(1, 4)
            insertMetadataStoreData(2, 5)

            //wait for that to be processed
            do {
                logger.info("Waiting for verified records count to change")
                Thread.sleep(1000)
            } while (reconciledRecordCount() < 3)

            //then
            assertThat(reconciledRecordCount()).isEqualTo(3)
            assertThat(allRecordCount()).isEqualTo(4)
            assertThat(recordsInHBase(qualifiedHbaseTableName)).isEqualTo(4)
        } catch (ex: Exception) {
            ex.printStackTrace()
            throw ex
        }
    }

    private fun emptyMetadataStoreTable() {
        logger.info("Start emptyMetadataStoreTable")
        with(metadatastoreConnection) {
            createStatement().use { it.execute("TRUNCATE ucfs") }
        }
        logger.info("End emptyMetadataStoreTable")
    }

    private fun emptyHBaseTable() {
        logger.info("Start emptyHBaseTable")
        val hbaseAdmin = hbaseConnection.admin
        disableHBaseTable(hbaseAdmin)
        logger.info("emptyHBaseTable: truncating table")
        hbaseAdmin.truncateTable(hbaseTableName(qualifiedHbaseTableName), false)

        enableHBaseTable(hbaseAdmin)

        logger.info("End emptyHBaseTable")
    }

    private fun enableHBaseTable(hbaseAdmin: Admin) {
        logger.info("Start enableHBaseTable")
        if (hbaseAdmin.isTableDisabled(hbaseTableName(qualifiedHbaseTableName))) {
            hbaseAdmin.enableTable(hbaseTableName(qualifiedHbaseTableName))
        }
        do {
            logger.info("emptyHBaseTable: waiting for table to be enabled")
            Thread.sleep(1000)
        } while (hbaseAdmin.isTableDisabled(hbaseTableName(qualifiedHbaseTableName)))
        logger.info("End enableHBaseTable")
    }

    private fun disableHBaseTable(hbaseAdmin: Admin) {
        logger.info("Start disableHBaseTable")
        if (hbaseAdmin.isTableEnabled(hbaseTableName(qualifiedHbaseTableName))) {
            hbaseAdmin.disableTableAsync(hbaseTableName(qualifiedHbaseTableName))
        }
        do {
            logger.info("emptyHBaseTable: waiting for table to be disabled")
            Thread.sleep(1000)
        } while (hbaseAdmin.isTableEnabled(hbaseTableName(qualifiedHbaseTableName)))
        logger.info("End disableHBaseTable")
    }

    private fun setupHBaseData(startIndex: Int, endIndex: Int) {
        enableHBaseTable(hbaseConnection.admin)
        hbaseTable(qualifiedHbaseTableName).use {
            val body = wellFormedValidPayload(hbaseNamespace, unqualifiedHbaseTableName)
            for (index in startIndex..endIndex) {
                val key = hbaseKey(index)
                it.put(Put(key).apply {
                    addColumn(columnFamily, columnQualifier, 1544799662000, body)
                })
            }
        }
    }

    private fun recordsInHBase(tableName: String)= hbaseTable(tableName).use {it.getScanner(Scan()).count()}

    private fun insertMetadataStoreData(startIndex: Int, endIndex: Int) {
        with (insertMetadatastoreRecord) {
            for (index in startIndex .. endIndex) {
                val aWeekAgo = Calendar.getInstance().apply {add(Calendar.DAY_OF_YEAR, -7)}
                setString(1, printableHbaseKey(index))
                setTimestamp(2, Timestamp(1544799662000))
                setString(3, kafkaTopic)
                setTimestamp(4, Timestamp(aWeekAgo.timeInMillis))
                setBoolean(5, false)
                addBatch()
            }
            executeBatch()
        }
    }

    private fun reconciledRecordCount(): Int = recordCount("SELECT COUNT(*) FROM ucfs WHERE reconciled_result=true")
    private fun allRecordCount(): Int = recordCount("SELECT COUNT(*) FROM ucfs")

    private fun recordCount(sql: String): Int =
        metadatastoreConnection.let { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(sql).use {
                    it.next()
                    it.getInt(1)
                }
            }
        }

    private fun printableHbaseKey(index: Int): String =
        MessageParser().printableKey(hbaseKey(index))

    private fun hbaseKey(index: Int)
        = MessageParser().generateKeyFromRecordBody(Parser.default().parse(StringBuilder("""{ "message": { "_id": $index } }""")) as JsonObject)

    private val hbaseNamespace = "claimant_advances"
    private val unqualifiedHbaseTableName = "advanceDetails"
    private val qualifiedHbaseTableName = "$hbaseNamespace:$unqualifiedHbaseTableName"

    private fun hbaseTable(name: String) = hbaseConnection.getTable(hbaseTableName(name))
    private fun hbaseTableName(name: String) = TableName.valueOf(name)

    //hbase_id, hbase_timestamp, topic_name, write_timestamp, reconciled_result
    private val insertMetadatastoreRecord by lazy {
        metadatastoreConnection.prepareStatement("""
            INSERT INTO ucfs (hbase_id, hbase_timestamp, topic_name, write_timestamp, reconciled_result)
            VALUES (?, ?, ?, ?, ?)
        """.trimIndent())
    }

}
