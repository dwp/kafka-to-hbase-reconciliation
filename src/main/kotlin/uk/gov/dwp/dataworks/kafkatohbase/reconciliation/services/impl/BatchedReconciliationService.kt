package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HBaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.logging.DataworksLogger
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

@Service
@Profile("RECONCILIATION")
@ExperimentalTime
class BatchedReconciliationService(
    private val hbaseRepository: HBaseRepository,
    private val metadataStoreRepository: MetadataStoreRepository,
    private val minimumAgeScale: Int,
    private val minimumAgeUnit: String
) : AbstractReconciliationService() {

    override fun startReconciliation() =
        metadataStoreRepository.reconcileRecords(unreconciledRecords())


    private fun unreconciledRecords(): List<UnreconciledRecord> = runBlocking {
        val rdbmsTimedValue = measureTimedValue {
            metadataStoreRepository.groupedUnreconciledRecords(minimumAgeScale, minimumAgeUnit);
        }

        logger.info("Queried metadatastore", "duration" to "${rdbmsTimedValue.duration}")

        val hbaseTimedValue = measureTimedValue {
            rdbmsTimedValue.value.map { (topic, records) ->
                async(Dispatchers.IO) {
                    hbaseRepository.recordsInHbase(topic, records)
                }
            }.awaitAll().flatten()
        }

        logger.info("Queried hbase", "duration" to "${hbaseTimedValue.duration}")
        hbaseTimedValue.value
    }

    companion object {
        private val logger = DataworksLogger.getLogger(AbstractReconciliationService::class.java.toString())
    }
}
