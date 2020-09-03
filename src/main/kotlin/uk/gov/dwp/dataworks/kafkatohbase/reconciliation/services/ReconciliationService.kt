package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services

import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

@Service
interface ReconciliationService {

    fun startReconciliationOnTimer()

    fun startReconciliation()

    fun reconcileRecords(records: List<Map<String, Any>>): Int
}
