
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.*
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
class ReconciliationIntegrationTest : StringSpec() {
    init {
        "Matching records are reconciled, mismatches are not" {
            coroutineScope {
                launch { populateHbase() }
                launch { populateMetadataStore() }
            }

            withTimeout(2.minutes) {
                while (reconciledRecordCount() < 1000) {
                    logger.info("Waiting for records to be reconciled")
                    delay(1.seconds)
                }
            }

            reconciledRecordCount() shouldBe 1000
            allRecordCount() shouldBe 2000

            hbaseConnection().use { connection ->
                for (topicIndex in 1..10) {
                    hbaseTableRecordCount(connection, "database:collection$topicIndex") shouldBe 100
                }
            }
        }
    }

    private suspend fun populateMetadataStore() = withContext(Dispatchers.IO) {
        logger.info("Putting lots of data into metadatastore")
        with(metadatastoreConnection) {
            val aWeekAgo = Calendar.getInstance().apply { add(Calendar.DAY_OF_YEAR, -7) }
            with(insertMetadatastoreRecordStatement(this)) {
                for (topicIndex in 1..10) {
                    logger.info("Adding records to metadatastore for topic 'db.database.collection$topicIndex'")
                    for (recordIndex in 1..200) {
                        setString(1, printableVolubleHbaseKey(topicIndex, recordIndex))
                        setTimestamp(2, Timestamp(1544799662000))
                        setString(3, "db.database.collection$topicIndex")
                        setTimestamp(4, Timestamp(aWeekAgo.timeInMillis))
                        setBoolean(5, false)
                        addBatch()
                    }
                    logger.info("Added records to metadatastore for topic 'db.database.collection$topicIndex'")
                }
                executeBatch()
            }
        }
        logger.info("Put lots of data into metadatastore")
    }

    private suspend fun populateHbase() = withContext(Dispatchers.IO) {
        logger.info("Putting lots of data into hbase")

        with(hbaseConnection()) {
            for (topicIndex in 1..10) {
                logger.info("Adding records to hbase for topic 'db.database.collection$topicIndex'")
                val tablename = hbaseTableName("database:collection$topicIndex")
                createTable(tablename)
                hbaseTable(this, "database:collection$topicIndex").use {
                    it.put((1..200 step 2).map { recordIndex ->
                        val body = wellFormedValidPayload("database", "collection$topicIndex")
                        val key = volubleHbaseKey(topicIndex, recordIndex)
                        Put(key).apply { addColumn(columnFamily, columnQualifier, 1544799662000, body) }
                    })
                }
                logger.info("Added records to hbase for topic 'db.database.collection$topicIndex'")
            }
        }
        logger.info("Put lots of data into hbase")
    }

    private fun HBaseConnection.createTable(tablename: TableName) {
        try {admin.createNamespace(NamespaceDescriptor.create(tablename.namespaceAsString).run { build() })}
        catch (e: Exception) {}

        try {
            admin.createTable(HTableDescriptor(tablename).apply {
                addFamily(HColumnDescriptor(columnFamily).apply {
                    maxVersions = Int.MAX_VALUE
                    minVersions = 1
                })
            })
        } catch (e: Exception) {}
    }

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationIntegrationTest::class.toString())
    }

    private val columnFamily = "cf".toByteArray()
    private val columnQualifier = "record".toByteArray()
    private val kafkaDb = "claimant-advances"
    private val kafkaCollection = "advanceDetails"
    private val kafkaTopic = "db.$kafkaDb.$kafkaCollection"

    private fun emptyMetadataStoreTable() {
        logger.info("Emptying metadatastore table")
        with(metadatastoreConnection) {
            createStatement().use {
                it.execute("TRUNCATE ucfs")
            }
        }
        logger.info("Emptied metadatastore table")
    }

    private suspend fun emptyHBaseTable(connection: HBaseConnection,  tableName: String) {
        logger.info("Emptying hbase table")
        disableHBaseTable(connection.admin, tableName)
        connection.admin.truncateTable(hbaseTableName(tableName), false)
        enableHBaseTable(connection.admin, tableName)
        logger.info("Emptied hbase table")
    }

    private suspend fun enableHBaseTable(hbaseAdmin: Admin, tableName: String) {
        if (hbaseAdmin.isTableDisabled(hbaseTableName(tableName))) {
            hbaseAdmin.enableTable(hbaseTableName(tableName))
        }
        while (hbaseAdmin.isTableDisabled(hbaseTableName(tableName))) {
            delay(1.seconds)
        }
    }

    private suspend fun disableHBaseTable(hbaseAdmin: Admin, tableName: String) {
        if (hbaseAdmin.isTableEnabled(hbaseTableName(tableName))) {
            hbaseAdmin.disableTableAsync(hbaseTableName(tableName))
        }
        while (hbaseAdmin.isTableEnabled(hbaseTableName(tableName))) {
            delay(1.seconds)
        }
    }

    private suspend fun setupHBaseData(connection: HBaseConnection, tableName: String, startIndex: Int, endIndex: Int) {
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
        val host = System.getenv("HBASE_ZOOKEEPER_QUORUM") ?: "localhost"
        val config = Configuration().apply {
            set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase")
            set(HConstants.ZOOKEEPER_QUORUM, host)
            setInt("hbase.zookeeper.port", 2181)
        }
        return ConnectionFactory.createConnection(HBaseConfiguration.create(config))
    }

    private fun printableHbaseKey(index: Int): String =
        MessageParser().printableKey(hbaseKey(index))

    private fun hbaseKey(index: Int) = MessageParser().generateKeyFromRecordBody(
        Parser.default().parse(StringBuilder("""{ "message": { "_id": $index } }""")) as JsonObject
    )

    private fun printableVolubleHbaseKey(topicIndex: Int, recordIndex: Int): String =
        MessageParser().printableKey(volubleHbaseKey(topicIndex, recordIndex))

    private fun volubleHbaseKey(topicIndex: Int, recordIndex: Int) = MessageParser().generateKeyFromRecordBody(
        Parser.default().parse(StringBuilder("""{ "message": { "_id": "$topicIndex/$recordIndex" } }""")) as JsonObject
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
