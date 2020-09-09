package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import org.springframework.context.annotation.Profile
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.ReconcilerConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.TrimReconciledRecordsService
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
@Profile("TRIM_RECONCILED_RECORDS")
class TrimReconciledRecordsServiceImpl(
    private val reconcilerConfiguration: ReconcilerConfiguration,
    private val metadataStoreRepository: MetadataStoreRepository
) : TrimReconciledRecordsService {

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationService::class.toString())
    }

    private var delayNeedsLogging = true

    fun logDelay() {
        if (delayNeedsLogging) {
            logger.info(
                "Running trim reconciled records with fixed delay between executions",
                "fixed_delay_millis" to reconcilerConfiguration.trimRecordsFixedDelayMillis!!
            )
            delayNeedsLogging = false
        }
    }

    //Executes with this millis delay between executions
    @Scheduled(fixedDelayString = "#{\${reconciler.trim.records.fixed.delay.millis}}", initialDelay = 1000)
    override fun trimReconciledRecordsOnTimer() {
        logDelay()
        trimReconciledRecords()
    }

    override fun trimReconciledRecords() {
        logger.info("Starting trim for reconciled records in the metadata store")

        val deletedCount = metadataStoreRepository.deleteRecordsOlderThanPeriod()

        logger.info("Finished trim for reconciled units",
            "deleted_count" to deletedCount.toString()
        )
    }
}
