package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Get
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.TextUtils
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService

@Service
class ReconciliationServiceImpl(
    private val hbaseConnection: Connection,
    private val metadatastoreConnection: java.sql.Connection
): ReconciliationService {

    override fun reconciliation() {
        println("$hbaseConnection")
        println("$metadatastoreConnection")
        fetchUnreconciledRecords().forEach {
            if (recordExistsInHbase(it["topic_name"], it["hbase_id"], it["hbase_timestamp"])) {
                reconcileRecord()
            }
        }
    }

    // retrieve records from metadata store
    private fun fetchUnreconciledRecords(): List<Map<String, String>> {
        val result = listOf<Map<String, String>>().toMutableList()
        val stmt = metadatastoreConnection.createStatement()
        val resultSet = stmt.executeQuery("SELECT * FROM ucfs WHERE write_timestamp > AND reconciled_result = false LIMIT 100")
        while (resultSet.next()) {
             result.add(mapOf<String, String>(
                 "id" to resultSet.getString("id"),
                 "hbase_id" to resultSet.getString("hbase_id"),
                 "hbase_timestamp" to resultSet.getString("hbase_timestamp"),
                 "write_timestamp" to resultSet.getString("write_timestamp"),
                 "correlation_id" to resultSet.getString("correlation_id"),
                 "topic_name" to resultSet.getString("topic_name"),
                 "kafka_partition" to resultSet.getString("kafka_partition"),
                 "kafka_offest" to resultSet.getString("kafka_offest"),
                 "reconciled_result" to resultSet.getString("reconciled_result"),
                 "reconciled_timestamp" to resultSet.getString("reconciled_timestamp")
            ))
        }

        return result
    }

    // check for items in HBase
    private fun recordExistsInHbase(table: String, id: String, version: String): Boolean {
        hbaseConnection.getTable(TableName.valueOf(tableName(table))).exists(Get(decodePrintable(id)))
        return true
    }

    // If found then update metadata store
    private fun reconcileRecord() {

    }

    private fun decodePrintable(printable: String): ByteArray {
        val checksum = printable.substring(0, 16)
        val rawish = checksum.replace(Regex("""\\x"""), "")
        val decoded = Hex.decodeHex(rawish)
        return decoded + printable.substring(16).toByteArray()
    }

    private val textUtils = TextUtils()

    private fun tableName(topic: String): String? {
        val matcher = textUtils.topicNameTableMatcher(topic)
        if (matcher != null) {
            val namespace = matcher.groupValues[1]
            val tableName = matcher.groupValues[2]
            return targetTable(namespace, tableName)
        } else {
            throw Exception("Could not derive table name from topic: $topic")
            //logger.error("Could not derive table name from topic", "topic", record.topic())
        }
    }

    private fun targetTable(namespace: String, tableName: String) =
        textUtils.coalescedName("$namespace:$tableName")?.replace("-", "_")



}
