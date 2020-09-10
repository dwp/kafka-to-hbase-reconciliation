package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.mock
import org.junit.jupiter.api.Test
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HBaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository

internal class BatchedReconciliationServiceTest {

    @Test
    fun startReconciliation() {
        val hbaseRepository = mock<HBaseRepository>()
        val metadataStoreRepository = mock<MetadataStoreRepository>()
        val reconciliationService = BatchedReconciliationService(hbaseRepository, metadataStoreRepository)
        reconciliationService.startReconciliation()
    }
}
