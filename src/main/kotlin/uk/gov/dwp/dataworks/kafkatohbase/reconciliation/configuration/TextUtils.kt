package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class TextUtils {

    @Value("\${hbase.table.pattern}")
    private lateinit var qualifiedTablePattern: String

//    private val qualifiedTablePattern = """^\w+\.([-\w]+)\.([-\w]+)$"""

    private val coalescedNames = mapOf("agent_core:agentToDoArchive" to "agent_core:agentToDo")

    fun topicNameTableMatcher(topicName: String): MatchResult? {
        println(qualifiedTablePattern)
        return Regex(qualifiedTablePattern).find(topicName)
    }

    fun coalescedName(tableName: String) = tableName
//        if (coalescedNames[tableName] != null) coalescedNames[tableName] else tableName
}
