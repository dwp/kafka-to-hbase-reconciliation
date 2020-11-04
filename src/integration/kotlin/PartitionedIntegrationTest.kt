
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.comparables.shouldBeGreaterThanOrEqualTo
import io.kotest.matchers.shouldBe
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
class PartitionedIntegrationTest : StringSpec() {

    init {
        "Matching records in partition are reconciled, mismatches are not" {
            val allRecords = topicCount * recordCount
            val partitionedRecordsCount = allRecords / 4 // Expect only p1 to have reconciled records

            val timeTaken = measureTime {
                withTimeout(15.minutes) {
                    launch { populateHbase() }
                    launch { populateMetadataStore() }
                    launch {
                        var recordsDone = 0
                        while (recordsDone < partitionedRecordsCount) {
                            recordsDone = reconciledRecordCount()
                            logger.info("Waiting for >= $partitionedRecordsCount records to be reconciled but is $recordsDone so far... ${Date()}")
                            delay(1.seconds)
                        }
                    }
                }
            }

            timeTaken shouldBeGreaterThan 15.seconds
            reconciledRecordCount() shouldBeGreaterThanOrEqualTo partitionedRecordsCount / 2
            allRecordCount() shouldBeGreaterThanOrEqualTo allRecords

            logger.info("partitionedRecordsCount: $partitionedRecordsCount")
            logger.info("reconciledRecordCount: ${reconciledRecordCount()}")

            logger.info("Checking records in metastore are updated...")

            // Expect p1 to be reconciled the records in this partition exist within hbase
            val resultOfPartition1Reconciled = recordCount("SELECT COUNT(*) FROM equalities PARTITION (p1) WHERE reconciled_result = true")
            resultOfPartition1Reconciled shouldBe 2500

            // Except p0 to have none reconciled as records in this partition don't exist in hbase
            val partition0Result = recordCount("SELECT COUNT(*) FROM equalities PARTITION (p0) WHERE reconciled_result = true")
            partition0Result shouldBe 0

            // Expect p2 to have none reconciled as records in this partition don't exist in hbase and the partition wasn't supplied to the service
            val partition2Result = recordCount("SELECT COUNT(*) FROM equalities PARTITION (p2) WHERE reconciled_result = true")
            partition2Result shouldBe 0

            // Expect p3 to have none reconciled as even though records in this partition do exist in hbase, the partition wasn't supplied to the service
            val partition3Result = recordCount("SELECT COUNT(*) FROM equalities PARTITION (p3) WHERE reconciled_result = true")
            partition3Result shouldBe 0

            withTimeout(5.minutes) {
                countLastModified()
            }

            logger.info("Done!")
        }
    }

    private tailrec fun countLastModified() {
        with (metadataStoreConnection) {
            createStatement().use {
                it.executeQuery("SELECT count(*) FROM equalities where last_checked_timestamp IS NOT NULL").use { results ->
                    if (results.next()) {
                        val count = results.getInt(1)
                        if (count >= (topicCount * recordCount) / 4) {
                            return
                        }
                    }
                }
            }
        }
        countLastModified()
    }

    private suspend fun populateMetadataStore() = withContext(Dispatchers.IO) {
        logger.info("Putting lots of data into metadatastore")
        with(metadataStoreConnection) {
            with(insertMetadataStoreRecordStatement(this)) {
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

    private fun HBaseConnection.ensureTable(tableNameAsString: String) {
        val tableName = hbaseTableName(tableNameAsString)
        if (!admin.tableExists(tableName)) {
            if (!admin.listNamespaceDescriptors().map { it.name }.contains(tableName.namespaceAsString)) {
                admin.createNamespace(NamespaceDescriptor.create(tableName.namespaceAsString).run { build() })
            }
            admin.createTable(HTableDescriptor(tableName).apply {
                addFamily(HColumnDescriptor(columnFamily).apply {
                    maxVersions = Int.MAX_VALUE
                    minVersions = 1
                    regionReplication = 3
                })
            })
        }
    }

    private val columnFamily = "cf".toByteArray()
    private val columnQualifier = "record".toByteArray()

    private fun reconciledRecordCount(): Int = recordCount("SELECT COUNT(*) FROM equalities WHERE reconciled_result=true")
    private fun allPartitionRecordCount(): Int = recordCount("SELECT COUNT(*) FROM equalities partition (p1)")
    private fun allRecordCount(): Int = recordCount("SELECT COUNT(*) FROM equalities")

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

    private fun insertMetadataStoreRecordStatement(connection: Connection) =
        connection.prepareStatement(
            """
            INSERT INTO equalities (hbase_id, hbase_timestamp, topic_name, reconciled_result)
            VALUES (?, ?, ?, ?)
        """.trimIndent()
        )

    private val topicCount = 10
    private val recordCount = 1000

    companion object {
        val logger = DataworksLogger.getLogger(PartitionedIntegrationTest::class.toString())
    }
}
