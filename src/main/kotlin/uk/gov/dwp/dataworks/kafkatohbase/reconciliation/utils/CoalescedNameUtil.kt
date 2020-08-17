package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils

import org.springframework.stereotype.Component

@Component
class CoalescedNameUtil {

    private val coalescedNames: Map<String, String> = mapOf("agent_core:agentToDoArchive" to "agent_core:agentToDo")

    fun coalescedName(tableName: String) = coalescedNames[tableName] ?: tableName
}
