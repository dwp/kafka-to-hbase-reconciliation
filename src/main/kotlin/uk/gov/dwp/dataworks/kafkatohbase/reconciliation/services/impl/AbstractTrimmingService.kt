package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import org.springframework.scheduling.annotation.Scheduled
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.TrimReconciledRecordsService

abstract class AbstractTrimmingService(private val trimRecordsFixedDelayMillis: Long):
    TrimReconciledRecordsService {

    @Scheduled(fixedDelayString = "#{\${reconciler.trim.records.fixed.delay.millis}}", initialDelay = 1000)
    override fun trimReconciledRecordsOnTimer() {
        logDelay()
        trimReconciledRecords()
    }

    private var delayNeedsLogging = true

    private fun logDelay() {
        if (delayNeedsLogging) {
            TrimReconciledRecordsServiceImpl.logger.info(
                "Running trim reconciled records with fixed delay between executions",
                "fixed_delay_millis" to "$trimRecordsFixedDelayMillis"
            )
            delayNeedsLogging = false
        }
    }
}
