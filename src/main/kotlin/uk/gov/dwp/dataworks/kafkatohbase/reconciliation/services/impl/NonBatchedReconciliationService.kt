package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HBaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ScheduledReconciliationService
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.sql.Timestamp

@Service
@Profile("!BATCHED")
class NonBatchedReconciliationService(private val repository: HBaseRepository,
                                      private val metadataStoreRepository: MetadataStoreRepository):
    AbstractReconciliationService() {

    override fun startReconciliation() {
        logger.info("Starting reconciliation of metadata store records")
        val recordsToReconcile = metadataStoreRepository.fetchUnreconciledRecords()
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

    private fun reconcileRecords(records: List<Map<String, Any>>): Int {
        var totalRecordsReconciled = 0

        records.forEach { record ->
            val topicName = record["topic_name"] as String
            val hbaseId = record["hbase_id"] as String
            val hbaseTimestamp = record["hbase_timestamp"] as Timestamp

            if (repository.recordExistsInHBase(topicName, hbaseId, hbaseTimestamp.time)) {
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
                        "hbase_timestamp" to hbaseTimestamp.toString()
                )
            }
        }

        return totalRecordsReconciled
    }

    companion object {
        val logger = DataworksLogger.getLogger(ScheduledReconciliationService::class.toString())
    }
}
