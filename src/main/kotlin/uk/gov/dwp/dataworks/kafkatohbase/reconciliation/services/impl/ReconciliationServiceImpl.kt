package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HBaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
class ReconciliationServiceImpl(
    private val HBaseRepository: HBaseRepository,
    private val metadataStoreRepository: MetadataStoreRepository) : ReconciliationService {

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationService::class.toString())
    }

    //Executes each X millis after the last execution
    @Scheduled(fixedDelayString="#{\${reconciler.fixed.delay.millis}}")
    //@Scheduled(fixedDelayString="5000")
    override fun startReconciliationOnTimer() {
        startReconciliation()
    }

    override fun startReconciliation() {
        logger.info("Starting reconciliation of metadata store records")
        val recordsToReconcile = metadataStoreRepository.fetchUnreconciledRecords()
        logger.info("Found records to reconcile",
                "records_to_reconcile" to recordsToReconcile.size.toString())

        if (recordsToReconcile.isNotEmpty()) {
            val totalRecordsReconciled = reconcileRecords(recordsToReconcile)

            logger.info(
                    "Finished reconciliation for metadata store",
                    "records_to_reconcile" to recordsToReconcile.size.toString(),
                    "total_records_reconciled" to totalRecordsReconciled.toString()
            )
        } else {
            logger.info("There are no records to be reconciled")
        }
    }

    override fun reconcileRecords(records: List<Map<String, Any>>): Int {
        var totalRecordsReconciled = 0

        records.forEach { record ->
            val topicName = record["topic_name"] as String
            val hbaseId = record["hbase_id"] as String
            val hbaseTimestamp = record["hbase_timestamp"] as Long

            if (HBaseRepository.recordExistsInHBase(topicName, hbaseId, hbaseTimestamp)) {
                logger.info("Reconcilling record",
                        "topic_name" to topicName,
                        "hbase_id" to hbaseId,
                        "hbase_timestamp" to hbaseTimestamp.toString())
                metadataStoreRepository.reconcileRecord(topicName)
                totalRecordsReconciled++
            } else {
                logger.warn("Reconciliation failed for topic as it does not exist in hbase",
                        "topic_name" to topicName,
                        "hbase_id" to hbaseId,
                        "hbase_timestamp" to hbaseTimestamp.toString()
                )
            }
        }

        return totalRecordsReconciled
    }
}
