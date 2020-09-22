package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ScheduledReconciliationService
import uk.gov.dwp.dataworks.logging.DataworksLogger
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

abstract class AbstractReconciliationService : ScheduledReconciliationService {

    @ExperimentalTime
    @Scheduled(fixedDelayString = "#{\${reconciler.fixed.delay.millis}}", initialDelay = 1000)
    override fun startScheduledReconciliation() {
        logDelay()
        val duration = measureTime { startReconciliation() }
        logger.info("Scheduled reconciliation finished", "duration" to "$duration")
    }

    private fun logDelay() {
        if (delayNeedsLogging) {
            logger.info(
                "Running reconciliation with fixed delay between executions",
                "fixed_delay_millis" to reconciliationDelayMillis
            )
            delayNeedsLogging = false
        }
    }

    @Value("\${reconciler.fixed.delay.millis}")
    lateinit var reconciliationDelayMillis: String
    private var delayNeedsLogging = true

    companion object {
        val logger = DataworksLogger.getLogger(AbstractReconciliationService::class.toString())
    }
}
