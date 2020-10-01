
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import uk.gov.dwp.dataworks.logging.DataworksLogger
import utility.MessageParser
import utility.wellFormedValidPayload
import java.sql.Connection
import java.sql.DriverManager
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.minutes
import kotlin.time.seconds
import org.apache.hadoop.hbase.client.Connection as HBaseConnection

@ExperimentalTime
class ReconciliationIntegrationTest : StringSpec() {
    init {
        "Matching records are reconciled, mismatches are not" {
            val timeTaken = measureTime {
                withTimeout(15.minutes) {
                    launch { populateHbase() }
                    launch { populateMetadataStore() }
                    launch {
                        while (reconciledRecordCount() < ((topicCount * recordCount) / 2)) {
                            logger.info("Waiting for records to be reconciled")
                            delay(1.seconds)
                        }
                    }
                }
            }

            timeTaken shouldBeGreaterThan 15.seconds
            allRecordCount() shouldBe topicCount * recordCount

            with (metadatastoreConnection) {
                createStatement().use { statement ->
                    statement.executeQuery("SELECT hbase_id, reconciled_result FROM ucfs").use {
                        while (it.next()) {
                            val hbaseId = it.getString("hbase_id")
                            val reconciledResult = it.getBoolean("reconciled_result")
                            val matchResult = Regex("""\{"id":"\d+/(?<recordno>\d+)"}""").find(hbaseId)
                            matchResult shouldNotBe null
                            if (matchResult != null) {
                                reconciledResult shouldBe matchResult.groupValues[1].toInt().isOdd()
                            }
                        }
                    }
                }
            }
        }
    }

    private suspend fun populateMetadataStore() = withContext(Dispatchers.IO) {
        logger.info("Putting lots of data into metadatastore")
        with(metadatastoreConnection) {
            with(insertMetadatastoreRecordStatement(this)) {
                for (topicIndex in 1..topicCount) {
                    logger.info("Adding records to metadatastore for topic 'db.database.collection$topicIndex'")
                    for (recordIndex in 1..recordCount) {
                        setString(1, printableHbaseKey(topicIndex, recordIndex))
                        setLong(2, 1544799662000L)
                        setString(3, "db.database.collection$topicIndex")
                        setBoolean(4, false)
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

        hbaseConnection().use { connection ->
            for (topicIndex in 1..topicCount) {
                logger.info("Adding records to hbase for topic 'db.database.collection$topicIndex'")
                connection.ensureTable(hbaseTableNameString(topicIndex))
                hbaseTable(connection, hbaseTableNameString(topicIndex)).use {
                    it.put((1..recordCount step 2).map { recordIndex ->
                        val body = wellFormedValidPayload("database", "collection$topicIndex")
                        val key = hbaseKey(topicIndex, recordIndex)
                        Put(key).apply { addColumn(columnFamily, columnQualifier, 1544799662000, body) }
                    })
                }
                logger.info("Added records to hbase for topic 'db.database.collection$topicIndex'")
            }
        }

        logger.info("Put lots of data into hbase")
    }

    private fun hbaseTableNameString(topicIndex: Int) = "database:collection$topicIndex"

    private fun HBaseConnection.ensureTable(tablenameAsString: String) {
        val tablename = hbaseTableName(tablenameAsString)
        if (!admin.tableExists(tablename)) {
            if (!admin.listNamespaceDescriptors().map { it.name }.contains(tablename.namespaceAsString)) {
                admin.createNamespace(NamespaceDescriptor.create(tablename.namespaceAsString).run { build() })
            }
            admin.createTable(HTableDescriptor(tablename).apply {
                addFamily(HColumnDescriptor(columnFamily).apply {
                    maxVersions = Int.MAX_VALUE
                    minVersions = 1
                })
            })
        }
    }

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationIntegrationTest::class.toString())
    }

    private val columnFamily = "cf".toByteArray()
    private val columnQualifier = "record".toByteArray()

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

    private fun printableHbaseKey(topicIndex: Int, recordIndex: Int): String =
        MessageParser().printableKey(hbaseKey(topicIndex, recordIndex))

    private fun hbaseKey(topicIndex: Int, recordIndex: Int) = MessageParser().generateKeyFromRecordBody(
        Parser.default().parse(StringBuilder("""{ "message": { "_id": "$topicIndex/$recordIndex" } }""")) as JsonObject
    )

    private fun hbaseTable(connection: org.apache.hadoop.hbase.client.Connection, name: String) =
        connection.getTable(hbaseTableName(name))

    private fun hbaseTableName(name: String) = TableName.valueOf(name)

    private fun insertMetadatastoreRecordStatement(connection: Connection) =
        connection.prepareStatement(
            """
            INSERT INTO ucfs (hbase_id, hbase_timestamp, topic_name, reconciled_result)
            VALUES (?, ?, ?, ?)
        """.trimIndent()
        )

    private fun Int.isOdd() = this % 2 == 1

    private val topicCount = 10
    private val recordCount = 1000
}

