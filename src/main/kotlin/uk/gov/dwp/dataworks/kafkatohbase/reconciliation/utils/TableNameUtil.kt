package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils

import org.apache.commons.codec.binary.Hex
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.HbaseConfiguration

@Component
class TableNameUtil(private val hbaseConfiguration: HbaseConfiguration,
                    private val coalescedNameUtil: CoalescedNameUtil) {

    fun getTableNameFromTopic(topic: String): String? {
        val matcher = topicNameTableMatcher(topic)
        if (matcher != null) {
            val namespace = matcher.groupValues[1]
            val tableName = matcher.groupValues[2]
            return targetTable(namespace, tableName)
        } else {
            throw Exception("Could not derive table name from topic: $topic")
        }
    }

    fun topicNameTableMatcher(topicName: String): MatchResult? {
        val qualifiedTablePattern = hbaseConfiguration.qualifiedTablePattern()
        return Regex(qualifiedTablePattern).find(topicName)
    }

    fun decodePrintable(printable: String): ByteArray {
        val checksum = printable.substring(0, 16)
        val rawish = checksum.replace(Regex("""\\x"""), "")
        val decoded = Hex.decodeHex(rawish)
        return decoded + printable.substring(16).toByteArray()
    }

    private fun targetTable(namespace: String, tableName: String) =
            coalescedNameUtil.coalescedName("$namespace:$tableName").replace("-", "_")
}
