import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import uk.gov.dwp.dataworks.logging.DataworksLogger
import utility.MessageParser
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.minutes
import kotlin.time.seconds
import org.apache.hadoop.hbase.client.Connection as HBaseConnection

@ExperimentalTime
class TrimReconciledIntegrationKoTest : StringSpec() {

    init {
        "Older reconciled records are deleted, unreconciled are left" {
            emptyMetadataStoreTable()
            val recordsInMetadataStore = allRecordCount()

            recordsInMetadataStore shouldBe 0

            insertMetadataStoreData(0, 1)
            insertReconciledMetadataStoreData(2, 5)

            withTimeout(1.minutes) {
                while (reconciledRecordCount() < 4) {
                    logger.info("Waiting for verified records count to change")
                    delay(1.seconds)
                }
            }

            allRecordCount() shouldBe 2
        }
    }

    companion object {
        val logger = DataworksLogger.getLogger(TrimReconciledIntegrationKoTest::class.toString())
    }

    private val kafkaDb = "claimant-advances"
    private val kafkaCollection = "advanceDetails"
    private val kafkaTopic = "db.$kafkaDb.$kafkaCollection"

    private fun emptyMetadataStoreTable() {
        with(metadataStoreConnection) {
            createStatement().use {
                it.execute("""TRUNCATE ucfs""")
            }
        }
    }

    private fun insertMetadataStoreData(startIndex: Int, endIndex: Int) {
        with(metadataStoreConnection) {
            val aWeekAgo = Calendar.getInstance().apply { add(Calendar.DAY_OF_YEAR, -7) }
            with(insertMetadataStoreRecordStatement(this)) {
                for (index in startIndex..endIndex) {
                    setString(1, printableHbaseKey(index))
                    setTimestamp(2, Timestamp(1544799662000))
                    setString(3, kafkaTopic)
                    setTimestamp(4, Timestamp(aWeekAgo.timeInMillis))
                    setTimestamp(5, null)
                    setBoolean(6, false)
                    addBatch()
                }
                executeBatch()
            }
        }
    }

    private fun insertReconciledMetadataStoreData(startIndex: Int, endIndex: Int) {
        with(metadataStoreConnection) {
            val aWeekAgo = Calendar.getInstance().apply { add(Calendar.DAY_OF_YEAR, -7) }
            val twoWeeksAgo = Calendar.getInstance().apply { add(Calendar.DAY_OF_YEAR, -14) }
            with(insertMetadataStoreRecordStatement(this)) {
                for (index in startIndex..endIndex) {
                    setString(1, printableHbaseKey(index))
                    setTimestamp(2, Timestamp(1544799662000))
                    setString(3, kafkaTopic)
                    setTimestamp(4, Timestamp(twoWeeksAgo.timeInMillis))
                    setTimestamp(5, Timestamp(aWeekAgo.timeInMillis))
                    setBoolean(6, true)
                    addBatch()
                }
                executeBatch()
            }
        }
    }

    private fun reconciledRecordCount(): Int = recordCount("SELECT COUNT(*) FROM ucfs WHERE reconciled_result=true")
    private fun allRecordCount(): Int = recordCount("SELECT COUNT(*) FROM ucfs")

    private fun recordCount(sql: String): Int =
        with(metadataStoreConnection) {
            createStatement().use { statement ->
                statement.executeQuery(sql).use {
                    it.next()
                    it.getInt(1)
                }
            }
        }

    private val metadataStoreConnection: Connection by lazy {
        DriverManager.getConnection("jdbc:mysql://metadatastore:3306/metadatastore", Properties().apply {
            setProperty("user", "reconciliationwriter")
            setProperty("password", "my-password")
        })
    }

    private fun printableHbaseKey(index: Int): String =
        MessageParser().printableKey(hbaseKey(index))

    private fun hbaseKey(index: Int) = MessageParser().generateKeyFromRecordBody(
        Parser.default().parse(StringBuilder("""{ "message": { "_id": $index } }""")) as JsonObject
    )

    private fun insertMetadataStoreRecordStatement(connection: Connection) =
        connection.prepareStatement(
            """
                INSERT INTO ucfs (hbase_id, hbase_timestamp, topic_name, write_timestamp, reconciled_timestamp, reconciled_result)
                VALUES (?, ?, ?, ?, ?, ?)
            """.trimIndent()
        )
}
