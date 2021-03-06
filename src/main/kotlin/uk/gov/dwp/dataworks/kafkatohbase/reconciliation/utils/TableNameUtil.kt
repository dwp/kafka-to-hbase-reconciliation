package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils

import org.apache.commons.codec.binary.Hex
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Component
@Profile("HBASE")
class TableNameUtil(private val coalescedNameUtil: CoalescedNameUtil) {

    @Value("\${hbase.table.pattern}")
    lateinit var qualifiedTablePattern: String

    fun getTableNameFromTopic(topic: String): String? {
        val matcher = topicNameTableMatcher(topic)
        if (matcher != null) {
            val namespace = matcher.groupValues[1]
            val tableName = matcher.groupValues[2]
            return targetTable(namespace, tableName)
        } else {
            throw Exception("Could not derive table name from topic: '$topic', pattern: '$qualifiedTablePattern'")
        }
    }

    fun topicNameTableMatcher(topicName: String): MatchResult? {
        val qualifiedTablePattern = qualifiedTablePattern
        return Regex(qualifiedTablePattern).find(topicName)
    }

    fun decodePrintable(printable: String): ByteArray {
        val checksum = printable.substring(0, 16)
        val rawish = checksum.replace(Regex("""\\x"""), "")
        val decoded = Hex.decodeHex(rawish)
        return decoded + printable.substring(16).toByteArray()
    }

    fun targetTable(namespace: String, tableName: String) =
        coalescedNameUtil.coalescedName("$namespace:$tableName")
            .replace("-", "_").replace(".", "_")
}
