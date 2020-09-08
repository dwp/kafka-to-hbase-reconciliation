package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HbaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
@Profile("RECONCILIATION")
class ReconciliationServiceImpl(
        private val hbaseRepository: HbaseRepository,
        private val metadataStoreRepository: MetadataStoreRepository) : ReconciliationService {

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationService::class.toString())
    }

    override fun startReconciliation() {

        val recordsToReconcile = metadataStoreRepository.fetchUnreconciledRecords()
        logger.info("Starting reconciliation of metadata store topics",
                "topics_to_reconcile" to recordsToReconcile.size.toString())

        if (recordsToReconcile.isNotEmpty()) {
            val totalRecordsReconciled = reconcileRecords(recordsToReconcile)

            logger.info(
                    "Finished reconciliation for topics from metadata store",
                    "topics_to_reconcile" to recordsToReconcile.size.toString(),
                    "total_topics_reconciled" to totalRecordsReconciled.toString()
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

            if (hbaseRepository.recordExistsInHbase(topicName, hbaseId, hbaseTimestamp)) {
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
