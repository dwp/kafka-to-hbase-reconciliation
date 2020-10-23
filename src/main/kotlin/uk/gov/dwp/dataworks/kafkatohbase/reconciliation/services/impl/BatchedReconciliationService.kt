package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import kotlinx.coroutines.*
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
    private val minimumAgeUnit: String,
    private val lastCheckedScale: Int,
    private val lastCheckedUnit: String): AbstractReconciliationService() {

    override fun startReconciliation() {
        unreconciledRecords().let {(inHBase, notInHbase) ->
            runBlocking {
                launch(Dispatchers.IO) { metadataStoreRepository.reconcileRecords(inHBase) }
                launch(Dispatchers.IO) { metadataStoreRepository.recordLastChecked(notInHbase) }
            }
        }
    }

    private fun unreconciledRecords(): Pair<List<UnreconciledRecord>, List<UnreconciledRecord>> = runBlocking {
        val rdbmsTimedValue = measureTimedValue {
            metadataStoreRepository.groupedUnreconciledRecords(minimumAgeScale, minimumAgeUnit, lastCheckedScale, lastCheckedUnit)
        }

        logger.info("Queried metadatastore", "duration" to "${rdbmsTimedValue.duration}")

        val hbaseTimedValue = measureTimedValue {
            rdbmsTimedValue.value.map { (topic, records) ->
                async(Dispatchers.IO) {
                    hbaseRepository.recordsInHbase(topic, records)
                }
            }.awaitAll()
        }

        logger.info("Queried hbase", "duration" to "${hbaseTimedValue.duration}")

        if (hbaseTimedValue.value.isNotEmpty()) {
            hbaseTimedValue.value.reduce { acc, pair ->
                Pair(acc.first + pair.first, acc.second + pair.second)
            }
        }
        else {
            Pair(listOf(), listOf())
        }
    }

    companion object {
        private val logger = DataworksLogger.getLogger(AbstractReconciliationService::class.java.toString())
    }
}
