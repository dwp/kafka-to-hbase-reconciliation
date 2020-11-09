package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.exceptions.OptimiseTableFailedException

interface MetadataStoreRepository {
    fun groupedUnreconciledRecords(minAgeSize: Int, minAgeUnit: String,
                                   lastCheckedScale: Int, lastCheckedUnit: String): Map<String, List<UnreconciledRecord>>
    fun reconcileRecords(unreconciled: List<UnreconciledRecord>)
    fun deleteRecordsOlderThanPeriod(trimReconciledScale: String, trimReconciledUnit: String): Int
    fun deleteAllReconciledRecords(deletedAccumulation: Int = 0): Int
    @Throws(OptimiseTableFailedException::class)
    fun optimizeTable(): Boolean
    fun recordLastChecked(batch: List<UnreconciledRecord>)
}
