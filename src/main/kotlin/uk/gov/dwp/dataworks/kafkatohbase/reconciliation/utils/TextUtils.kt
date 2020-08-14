package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils

import org.apache.commons.codec.binary.Hex
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.HbaseConfiguration

@Component
class TextUtils(hbaseConfiguration: HbaseConfiguration) {

    private val qualifiedTablePattern = hbaseConfiguration.qualifiedTablePattern()

    private val coalescedNames: Map<String, String> = mapOf("agent_core:agentToDoArchive" to "agent_core:agentToDo")

    fun topicNameTableMatcher(topicName: String): MatchResult? {
        return Regex(qualifiedTablePattern).find(topicName)
    }

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

    fun decodePrintable(printable: String): ByteArray {
        val checksum = printable.substring(0, 16)
        val rawish = checksum.replace(Regex("""\\x"""), "")
        val decoded = Hex.decodeHex(rawish)
        return decoded + printable.substring(16).toByteArray()
    }

    private fun coalescedName(tableName: String) = coalescedNames[tableName] ?: tableName

    private fun targetTable(namespace: String, tableName: String) =
            coalescedName("$namespace:$tableName").replace("-", "_")
}
