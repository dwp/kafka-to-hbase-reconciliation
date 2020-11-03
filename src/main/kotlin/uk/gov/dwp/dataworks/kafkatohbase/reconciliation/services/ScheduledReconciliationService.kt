package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services

interface ScheduledReconciliationService: ReconciliationService {
    fun startScheduled()
}
