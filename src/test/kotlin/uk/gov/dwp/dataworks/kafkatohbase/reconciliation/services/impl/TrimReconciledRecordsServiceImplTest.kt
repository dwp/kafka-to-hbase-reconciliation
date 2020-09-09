package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.whenever
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.ReconcilerConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [TrimReconciledRecordsServiceImpl::class])
@ActiveProfiles("TRIM_RECONCILED_RECORDS")
class TrimReconciledRecordsServiceImplTest {

    @Autowired
    private lateinit var service: TrimReconciledRecordsServiceImpl

    @MockBean
    private lateinit var reconcilerConfiguration: ReconcilerConfiguration

    @MockBean
    private lateinit var metadataStoreRepository: MetadataStoreRepository

    @Test
    fun willDeleteRecordsOlderThanScaleAndUnit() {

        whenever(metadataStoreRepository.deleteRecordsOlderThanPeriod()).thenReturn(1)

        service.trimReconciledRecords()


    }
}