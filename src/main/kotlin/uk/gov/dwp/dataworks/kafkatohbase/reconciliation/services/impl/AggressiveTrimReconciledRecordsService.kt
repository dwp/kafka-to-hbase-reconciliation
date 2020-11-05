package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.exceptions.OptimiseTableFailedException
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
@Profile("TRIM_RECONCILED_RECORDS")
class AggressiveTrimReconciledRecordsService(
    trimRecordsFixedDelayMillis: Long,
    private val metadataStoreRepository: MetadataStoreRepository,
    private val optimizeAfterDelete: Boolean):
    AbstractTrimmingService(trimRecordsFixedDelayMillis) {

    override fun trimReconciledRecords() {
        logger.info("Starting trim for reconciled records in the metadata store")
        val deletedCount = metadataStoreRepository.deleteAllReconciledRecords()
        logger.info("Finished trim for reconciled units","deleted_count" to "$deletedCount")

        if (deletedCount > 0 && optimizeAfterDelete) {
            logger.info("Optimising table")

            for (attempt in 0..1) {

                try {
                    val succeeded = metadataStoreRepository.optimizeTable()
                    if (succeeded) {
                        logger.info("Optimisation successful", "attempt" to "$attempt")
                        return
                    } else {
                        logger.info("Optimisation has failed without exception", "attempt" to "$attempt")
                    }
                } catch (e: OptimiseTableFailedException) {
                    logger.error("Optimisation has failed due to exception", "attempt" to "$attempt", "exception" to "$e")
                }

            }
        }
        else {
            logger.info("Not optimising table",
                "optimize_after_delete" to "$optimizeAfterDelete", "deleted_count" to "$deletedCount")
        }
    }

    companion object {
        val logger = DataworksLogger.getLogger(AggressiveTrimReconciledRecordsService::class.toString())
    }
}
