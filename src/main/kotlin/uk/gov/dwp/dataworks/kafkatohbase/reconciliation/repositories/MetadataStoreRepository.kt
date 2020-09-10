package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord

interface MetadataStoreRepository {
    fun groupedUnreconciledRecords(): Map<String, List<UnreconciledRecord>>
    fun reconcileRecord(topicName: String, hbaseId: String, version: Long)
    fun reconcileRecords(unreconciled: List<UnreconciledRecord>)
    fun fetchUnreconciledRecords(): List<Map<String, Any>>
    fun deleteRecordsOlderThanPeriod(trimReconciledScale: String, trimReconciledUnit: String): Int
}
