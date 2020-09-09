package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HBaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository

@Service
@Profile("BATCHED")
class BatchedReconciliationService(private val hbaseRepository: HBaseRepository,
                                   private val metadataStoreRepository: MetadataStoreRepository):
    AbstractReconciliationService() {

    override fun startReconciliation()= metadataStoreRepository.reconcileRecords(unreconciledRecords())

    private fun unreconciledRecords(): List<UnreconciledRecord> =
        metadataStoreRepository.groupedUnreconciledRecords().flatMap { hbaseRepository.recordsNotInHbase(it.key, it.value) }
}
