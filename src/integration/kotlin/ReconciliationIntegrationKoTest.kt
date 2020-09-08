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
class ReconciliationIntegrationKoTest : StringSpec() {
    init {
        "Matching records are reconciled, mismatches are not" {
            hbaseConnection().use {connection ->
                val hbaseTableName = "claimant_advances:advanceDetails"
                emptyHBaseTable(connection, hbaseTableName)
                emptyMetadataStoreTable()
                val recordsInMetadataStore = allRecordCount()
                val recordsInHBase = hbaseTableRecordCount(connection, hbaseTableName)
                recordsInMetadataStore shouldBe 0
                recordsInHBase shouldBe 0
                setupHBaseData(connection, hbaseTableName, 1, 4)
                insertMetadataStoreData(2, 5)

                withTimeout(1.minutes) {
                    while (reconciledRecordCount() < 3) {
                        logger.info("Waiting for verified records count to change")
                        delay(1.seconds)
                    }
                }

                reconciledRecordCount() shouldBe 3
                allRecordCount() shouldBe 4
                hbaseTableRecordCount(connection, hbaseTableName) shouldBe 4
            }
        }
    }

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationIntegrationKoTest::class.toString())
    }

    private val columnFamily = "cf".toByteArray()
    private val columnQualifier = "record".toByteArray()
    private val kafkaDb = "claimant-advances"
    private val kafkaCollection = "advanceDetails"
    private val kafkaTopic = "db.$kafkaDb.$kafkaCollection"


    private fun emptyMetadataStoreTable() {
        with(metadatastoreConnection) {
            createStatement().use {
                it.execute("TRUNCATE ucfs")
            }
        }
    }

    private fun emptyHBaseTable(connection: HBaseConnection,  tableName: String) {
        disableHBaseTable(connection.admin, tableName)
        connection.admin.truncateTable(hbaseTableName(tableName), false)
        enableHBaseTable(connection.admin, tableName)
    }

    private fun enableHBaseTable(hbaseAdmin: Admin, tableName: String) {
        if (hbaseAdmin.isTableDisabled(hbaseTableName(tableName))) {
            hbaseAdmin.enableTable(hbaseTableName(tableName))
        }
        do {
            Thread.sleep(1000)
        } while (hbaseAdmin.isTableDisabled(hbaseTableName(tableName)))
    }

    private fun disableHBaseTable(hbaseAdmin: Admin, tableName: String) {
        if (hbaseAdmin.isTableEnabled(hbaseTableName(tableName))) {
            hbaseAdmin.disableTableAsync(hbaseTableName(tableName))
        }
        do {
            Thread.sleep(1000)
        } while (hbaseAdmin.isTableEnabled(hbaseTableName(tableName)))
    }

    private fun setupHBaseData(connection: HBaseConnection, tableName: String, startIndex: Int, endIndex: Int) {
        enableHBaseTable(connection.admin, tableName)
        hbaseTable(connection, tableName).use {
            val (namespace, unqualifiedName) = tableName.split(":")
            val body = wellFormedValidPayload(namespace, unqualifiedName)
            for (index in startIndex..endIndex) {
                val key = hbaseKey(index)
                it.put(Put(key).apply {
                    addColumn(columnFamily, columnQualifier, 1544799662000, body)
                })
            }
        }
    }


    private fun hbaseTableRecordCount(connection: HBaseConnection, tableName: String) =
            hbaseTable(connection, tableName).use { it.getScanner(Scan()).count() }


    private fun insertMetadataStoreData(startIndex: Int, endIndex: Int) {
        with(metadatastoreConnection) {
            val aWeekAgo = Calendar.getInstance().apply { add(Calendar.DAY_OF_YEAR, -7) }
            with(insertMetadatastoreRecordStatement(this)) {
                for (index in startIndex..endIndex) {
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
    }

    private fun reconciledRecordCount(): Int = recordCount("SELECT COUNT(*) FROM ucfs WHERE reconciled_result=true")
    private fun allRecordCount(): Int = recordCount("SELECT COUNT(*) FROM ucfs")

    private fun recordCount(sql: String): Int =
        with(metadatastoreConnection) {
            createStatement().use { statement ->
                statement.executeQuery(sql).use {
                    it.next()
                    it.getInt(1)
                }
            }
        }

    private val metadatastoreConnection: Connection by lazy {
        DriverManager.getConnection("jdbc:mysql://metadatastore:3306/metadatastore", Properties().apply {
            setProperty("user", "reconciliationwriter")
            setProperty("password", "my-password")
        })
    }

    private fun hbaseConnection(): org.apache.hadoop.hbase.client.Connection {
        val config = Configuration().apply {
            set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase")
            set(HConstants.ZOOKEEPER_QUORUM, "localhost")
            setInt("hbase.zookeeper.port", 2181)
        }
        return ConnectionFactory.createConnection(HBaseConfiguration.create(config))
    }

    private fun printableHbaseKey(index: Int): String =
        MessageParser().printableKey(hbaseKey(index))

    private fun hbaseKey(index: Int) = MessageParser().generateKeyFromRecordBody(
        Parser.default().parse(StringBuilder("""{ "message": { "_id": $index } }""")) as JsonObject
    )


    private fun hbaseTable(connection: org.apache.hadoop.hbase.client.Connection, name: String) =
        connection.getTable(hbaseTableName(name))

    private fun hbaseTableName(name: String) = TableName.valueOf(name)

    private fun insertMetadatastoreRecordStatement(connection: Connection) =
        connection.prepareStatement(
            """
            INSERT INTO ucfs (hbase_id, hbase_timestamp, topic_name, write_timestamp, reconciled_result)
            VALUES (?, ?, ?, ?, ?)
        """.trimIndent()
        )


}

//    @Test
//    fun testWeCanEmptyHBase() {
//        try {
//            emptyHBaseTable()
//        } catch (ex: Exception) {
//            logger.error("Exception in testWeCanEmptyHBase", ex)
//            throw ex
//        }
//    }
//
//    @Test
//    fun testWeCanCheckHBase() {
//        try {
//            recordsInHBase()
//        } catch (ex: Exception) {
//            logger.error("Exception in testWeCanCheckHBase", ex)
//            throw ex
//        }
//    }
//
//    @Test
//    fun testWeCanEmptyMetadataStore() {
//        try {
//            emptyMetadataStoreTable()
//        } catch (ex: Exception) {
//            logger.error("Exception in testWeCanEmptyMetadataStore", ex)
//            throw ex
//        }
//    }
//
//    @Test
//    fun testWeCanFillHBase() {
//        try {
//            setupHBaseData(0, 0)
//        } catch (ex: Exception) {
//            logger.error("Exception in testWeCanFillHBase", ex)
//            throw ex
//        }
//    }
//
//    @Test
//    fun testWeCanFillMetastore() {
//        try {
//            setupMetadataStoreData(0, 0)
//        } catch (ex: Exception) {
//            logger.error("Exception in testWeCanFillMetastore", ex)
//            throw ex
//        }
//    }
//
//    @Test
//    fun testWeCanCheckMetastoreForReconciled() {
//        try {
//            reconciledRecordsInMetadataStore()
//        } catch (ex: Exception) {
//            logger.error("Exception in testWeCanCheckMetastoreForReconciled", ex)
//            throw ex
//        }
//    }
//
//    @Test
//    fun testWeCanCheckMetastore() {
//        try {
//            allRecordsInMetadataStore()
//        } catch (ex: Exception) {
//            logger.error("Exception in testWeCanCheckMetastore", ex)
//            throw ex
//        }
//    }

