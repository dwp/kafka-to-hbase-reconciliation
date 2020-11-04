package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Test
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository

class TrimReconciledRecordsServiceImplTest {

    @Test
    fun optimizesAfterTrimIfConfiguredAndDeletesPerformed() {
        val metadataStoreRepository = metadataStoreRepository(1)
        val trimmer = trimmer(metadataStoreRepository, true)
        trimmer.start()
        verify(metadataStoreRepository, times(1)).deleteAllReconciledRecords()
        verify(metadataStoreRepository, times(1)).optimizeTable()
        verifyNoMoreInteractions(metadataStoreRepository)
    }


    @Test
    fun doesNotOptimizeAfterTrimIfConfiguredAndNoDeletesPerformed() {
        val metadataStoreRepository = metadataStoreRepository(0)
        val trimmer = trimmer(metadataStoreRepository, true)
        trimmer.start()
        verify(metadataStoreRepository, times(1)).deleteAllReconciledRecords()
        verifyNoMoreInteractions(metadataStoreRepository)
    }

    @Test
    fun doesNotOptimizeAfterTrimIfNotConfiguredAndRecordsDeleted() {
        val metadataStoreRepository = metadataStoreRepository(1)
        val trimmer = trimmer(metadataStoreRepository, false)
        trimmer.start()
        verify(metadataStoreRepository, times(1)).deleteAllReconciledRecords()
        verifyNoMoreInteractions(metadataStoreRepository)
    }

    @Test
    fun doesNotOptimizeAfterTrimIfNotConfiguredAndNoRecordsDeleted() {
        val metadataStoreRepository = metadataStoreRepository(0)
        val trimmer = trimmer(metadataStoreRepository, false)
        trimmer.start()
        verify(metadataStoreRepository, times(1)).deleteAllReconciledRecords()
        verifyNoMoreInteractions(metadataStoreRepository)
    }

    private fun trimmer(metadataStoreRepository: MetadataStoreRepository, optimize: Boolean) =
        TrimmingServiceImpl(metadataStoreRepository, optimize)

    private fun metadataStoreRepository(numDeletes: Int) =
        mock<MetadataStoreRepository> {
            on { deleteAllReconciledRecords() } doReturn numDeletes
        }
}
