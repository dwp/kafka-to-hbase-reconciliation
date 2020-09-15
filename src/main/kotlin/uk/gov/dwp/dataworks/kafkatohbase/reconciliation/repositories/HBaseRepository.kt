package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord

interface HBaseRepository {
    fun recordsInHbase(topicName: String, records: List<UnreconciledRecord>): List<UnreconciledRecord>
}
