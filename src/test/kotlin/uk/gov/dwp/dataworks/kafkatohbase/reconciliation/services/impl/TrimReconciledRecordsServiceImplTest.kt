package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.*
import io.kotest.assertions.throwables.shouldThrow
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.exceptions.OptimiseTableFailedException
import java.lang.Exception


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
    fun willRetryToMaxRetriesIfOptimizeTableFails() {
        val metadataStoreRepository =
            mock<MetadataStoreRepository> {
                on { optimizeTable() } doReturn false
                on { deleteAllReconciledRecords() } doReturn 1
            }
        val trimmer = trimmer(metadataStoreRepository, true)
        trimmer.start()

        verify(metadataStoreRepository, times(1)).deleteAllReconciledRecords()
        verify(metadataStoreRepository, times(TrimmingServiceImpl.maxRetries + 1)).optimizeTable()
        verifyNoMoreInteractions(metadataStoreRepository)
    }

    @Test
    fun willRetryOptimizeTableToFailFirstTimeThenPassesSecondTime() {
        val metadataStoreRepository =
            mock<MetadataStoreRepository> {
                on { deleteAllReconciledRecords() } doReturn 1
            }
        whenever(metadataStoreRepository.optimizeTable()).thenReturn(false).thenReturn(true)

        val trimmer = trimmer(metadataStoreRepository, true)
        trimmer.start()

        verify(metadataStoreRepository, times(1)).deleteAllReconciledRecords()
        verify(metadataStoreRepository, times(TrimmingServiceImpl.maxRetries + 1)).optimizeTable()
        verifyNoMoreInteractions(metadataStoreRepository)
    }

    @Test
    fun willRetryOptimizeTableIfOptimizeFailsWithException() {
        val metadataStoreRepository =
            mock<MetadataStoreRepository> {
                on { deleteAllReconciledRecords() } doReturn 1
                on { optimizeTable() } doThrow OptimiseTableFailedException("string")
            }

        val trimmer = trimmer(metadataStoreRepository, true)
        trimmer.start()

        val e = shouldThrow<OptimiseTableFailedException> { metadataStoreRepository.optimizeTable()  }

        verify(metadataStoreRepository, times(1)).deleteAllReconciledRecords()
        verify(metadataStoreRepository, times(TrimmingServiceImpl.maxRetries + 1)).optimizeTable()
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
            on { optimizeTable() } doReturn true
        }
}
