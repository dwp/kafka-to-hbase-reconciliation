package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

class TextUtils {

    private val qualifiedTablePattern = Regex(Config.Hbase.qualifiedTablePattern)
    private val coalescedNames = mapOf("agent_core:agentToDoArchive" to "agent_core:agentToDo")

    fun topicNameTableMatcher(topicName: String) = qualifiedTablePattern.find(topicName)

    fun coalescedName(tableName: String) =
        if (coalescedNames[tableName] != null) coalescedNames[tableName] else tableName
}
