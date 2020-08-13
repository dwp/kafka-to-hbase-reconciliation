package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils

import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.HbaseConfiguration

@Component
class TextUtils(hbaseConfiguration: HbaseConfiguration) {

    private val qualifiedTablePattern = hbaseConfiguration.qualifiedTablePattern()

    private val coalescedNames: Map<String, String> = mapOf("agent_core:agentToDoArchive" to "agent_core:agentToDo")

    fun topicNameTableMatcher(topicName: String): MatchResult? {
        return Regex(qualifiedTablePattern).find(topicName)
    }

    fun coalescedName(tableName: String) = coalescedNames[tableName] ?: tableName
}
