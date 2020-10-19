package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
@Profile("TRIM_OLDER_RECONCILED_RECORDS")
class TrimReconciledRecordsServiceImpl(
    trimRecordsFixedDelayMillis: Long,
    private val metadataStoreRepository: MetadataStoreRepository,
    private val trimReconciledScale: String,
    private val trimReconciledUnit: String):
    AbstractTrimmingService(trimRecordsFixedDelayMillis) {

    override fun trimReconciledRecords() {
        logger.info("Starting trim for reconciled records in the metadata store")
        val deletedCount = metadataStoreRepository.deleteRecordsOlderThanPeriod(trimReconciledScale, trimReconciledUnit)
        logger.info("Finished trim for reconciled units", "deleted_count" to deletedCount.toString())
    }

    companion object {
        val logger = DataworksLogger.getLogger(TrimReconciledRecordsServiceImpl::class.toString())
    }
}
