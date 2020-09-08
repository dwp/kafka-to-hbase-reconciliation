package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HBaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.sql.Timestamp

@Service
class ReconciliationServiceImpl(
    private val hbaseRepository: HBaseRepository,
    private val metadataStoreRepository: MetadataStoreRepository) : ReconciliationService {

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationService::class.toString())
    }

    @Value("\${reconciler.fixed.delay.millis}")
    lateinit var reconciliationDelayMillis: String

    private var delayNeedsLogging = true

    fun logDelay() {
        if (delayNeedsLogging) {
            logger.info("Running reconciliation with fixed delay between executions",
                "fixed_delay_millis" to reconciliationDelayMillis
            )
            delayNeedsLogging = false
        }
    }

    //Executes with this millis delay between executions
    @Scheduled(fixedDelayString="#{\${reconciler.fixed.delay.millis}}", initialDelay = 1000)
    override fun startReconciliationOnTimer() {
        logDelay()
        startReconciliation()
    }

    fun startNonbatchedReconciliation() {
        logger.info("Starting reconciliation of metadata store records")
        val recordsToReconcile = metadataStoreRepository.fetchUnreconciledRecords()
        val groupedUnreconciledRecords = metadataStoreRepository.groupedUnreconciledRecords()
        if (recordsToReconcile.isNotEmpty()) {
            logger.info("Found records to reconcile",
                "records_to_reconcile" to recordsToReconcile.size.toString())
            val totalRecordsReconciled = reconcileRecords(recordsToReconcile)

            logger.debug(
                    "Finished reconciliation for metadata store",
                    "records_to_reconcile" to recordsToReconcile.size.toString(),
                    "total_records_reconciled" to totalRecordsReconciled.toString()
            )
        } else {
            logger.info("There are no records to be reconciled")
        }
    }

    override fun startReconciliation() {
        val groupedUnreconciledRecords = metadataStoreRepository.groupedUnreconciledRecords()
        if (groupedUnreconciledRecords.isNotEmpty()) {
            groupedUnreconciledRecords.forEach { topic, records ->
                val wtf = hbaseRepository.recordsExistInHBase(topic, records)

            }
        } else {
        }
    }

    override fun reconcileRecords(records: List<Map<String, Any>>): Int {
        var totalRecordsReconciled = 0

        records.forEach { record ->
            val topicName = record["topic_name"] as String
            val hbaseId = record["hbase_id"] as String
            val hbaseTimestamp = record["hbase_timestamp"] as Timestamp

            if (hbaseRepository.recordExistsInHBase(topicName, hbaseId, hbaseTimestamp.time)) {
                logger.info("Reconcilling record",
                        "topic_name" to topicName,
                        "hbase_id" to hbaseId,
                        "hbase_timestamp" to hbaseTimestamp.toString())
                metadataStoreRepository.reconcileRecord(topicName, hbaseId, hbaseTimestamp.time)
                totalRecordsReconciled++
            } else {
                logger.warn("Reconciliation failed for topic as it does not exist in hbase",
                        "topic_name" to topicName,
                        "hbase_id" to hbaseId,
                        "hbase_timestamp" to hbaseTimestamp.toString())
            }
        }

        return totalRecordsReconciled
    }
}
