package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import kotlinx.coroutines.*
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HBaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ScheduledReconciliationService
import uk.gov.dwp.dataworks.logging.DataworksLogger
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

@Service
@Profile("RECONCILIATION")
@ExperimentalTime
class ReconciliationServiceImpl(
    private val hbaseRepository: HBaseRepository,
    private val metadataStoreRepository: MetadataStoreRepository,
    private val minimumAgeScale: Int,
    private val minimumAgeUnit: String,
    private val lastCheckedScale: Int,
    private val lastCheckedUnit: String): ScheduledReconciliationService {

    @ExperimentalTime
    @Scheduled(fixedDelayString = "#{\${reconciler.fixed.delay.millis}}", initialDelay = 1000)
    override fun startScheduled() {
        logDelay()
        val duration = measureTime { start() }
        logger.info("Scheduled reconciliation finished", "duration" to "$duration")
    }

    @Value("\${reconciler.fixed.delay.millis}")
    lateinit var reconciliationDelayMillis: String
    private var delayNeedsLogging = true

    override fun start() {
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

    private fun logDelay() {
        if (delayNeedsLogging) {
            logger.info(
                "Running reconciliation with fixed delay between executions",
                "fixed_delay_millis" to reconciliationDelayMillis
            )
            delayNeedsLogging = false
        }
    }

    companion object {
        private val logger = DataworksLogger.getLogger(ReconciliationServiceImpl::class.java.toString())
    }
}
