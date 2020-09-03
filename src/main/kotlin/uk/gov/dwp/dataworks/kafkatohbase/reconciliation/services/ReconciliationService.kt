package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services

import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

@Service
interface ReconciliationService {

    @Scheduled(fixedDelayString="5000")
    fun startReconciliation()

    fun reconcileRecords(records: List<Map<String, Any>>): Int
}
