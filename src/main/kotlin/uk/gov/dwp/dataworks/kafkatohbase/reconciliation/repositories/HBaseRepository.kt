package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord

interface HBaseRepository {
    fun recordsNotInHbase(topicName: String, records: List<UnreconciledRecord>): List<UnreconciledRecord>
    fun recordExistsInHBase(topicName: String, id: String, version: Long): Boolean
}
