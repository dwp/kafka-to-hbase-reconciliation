package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.ReconcilerConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository

@ActiveProfiles("TRIM_RECONCILED_RECORDS")
class TrimReconciledRecordsServiceImplTest {

    @Test
    fun willDeleteRecordsOlderThanScaleAndUnit() {

        val trimReconciledScale = "1"
        val trimReconciledUnit = "DAY"

        val reconcilerConfiguration = mock<ReconcilerConfiguration>()
        val metadataStoreRepository = mock<MetadataStoreRepository> {
            on {
                deleteRecordsOlderThanPeriod(trimReconciledScale, trimReconciledUnit)
            } doReturn 1
        }

        val service = TrimReconciledRecordsServiceImpl(
            reconcilerConfiguration, metadataStoreRepository, trimReconciledScale, trimReconciledUnit
        )

        service.trimReconciledRecords()

        verify(metadataStoreRepository, times(1)).deleteRecordsOlderThanPeriod(trimReconciledScale, trimReconciledUnit)
    }
}
