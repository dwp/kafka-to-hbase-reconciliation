package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services

interface ReconciliationService {
    fun startReconciliationOnTimer()
    fun startReconciliation()
    fun reconcileRecords(records: List<Map<String, Any>>): Int
}