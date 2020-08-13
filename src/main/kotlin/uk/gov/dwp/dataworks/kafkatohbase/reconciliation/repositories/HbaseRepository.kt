package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Get
import org.springframework.stereotype.Repository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.TextUtils

@Repository
class HbaseRepository(val hbaseConnection: Connection,
                      private val textUtils: TextUtils) {

    fun recordExistsInHbase(table: String, id: String, version: Long): Boolean {
        return hbaseConnection.getTable(TableName.valueOf(tableName(table))).exists(Get(decodePrintable(id)))
    }

    private fun tableName(topic: String): String? {
        val matcher = textUtils.topicNameTableMatcher(topic)
        if (matcher != null) {
            val namespace = matcher.groupValues[1]
            val tableName = matcher.groupValues[2]
            return targetTable(namespace, tableName)
        } else {
            throw Exception("Could not derive table name from topic: $topic")
        }
    }

    private fun targetTable(namespace: String, tableName: String) =
            textUtils.coalescedName("$namespace:$tableName").replace("-", "_")

    private fun decodePrintable(printable: String): ByteArray {
        val checksum = printable.substring(0, 16)
        val rawish = checksum.replace(Regex("""\\x"""), "")
        val decoded = Hex.decodeHex(rawish)
        return decoded + printable.substring(16).toByteArray()
    }
}