package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository

class TrimReconciledRecordsServiceImplTest {

    @Test
    fun willDeleteRecordsOlderThanScaleAndUnit() {

        val trimReconciledScale = "1"
        val trimReconciledUnit = "DAY"

        val metadataStoreRepository = mock<MetadataStoreRepository> {
            on {
                deleteRecordsOlderThanPeriod(trimReconciledScale, trimReconciledUnit)
            } doReturn 1
        }

        val service = TrimReconciledRecordsServiceImpl(
            1000L, metadataStoreRepository, trimReconciledScale, trimReconciledUnit
        )

        service.trimReconciledRecords()

        verify(metadataStoreRepository, times(1)).deleteRecordsOlderThanPeriod(trimReconciledScale, trimReconciledUnit)
    }
}
