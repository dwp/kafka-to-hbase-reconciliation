package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.given
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HBaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl.HBaseRepositoryImpl
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl.MetadataStoreRepositoryImpl
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ScheduledReconciliationService
import java.sql.Timestamp

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [NonBatchedReconciliationService::class])
@ActiveProfiles("NON_BATCHED_RECONCILIATION")
class NonBatchedReconciliationServiceTests {

	@Autowired
	private lateinit var reconciliationService: NonBatchedReconciliationService

	@MockBean
	private lateinit var metadataStoreRepository: MetadataStoreRepositoryImpl

	@MockBean
	private lateinit var hbaseRepository: HBaseRepository

	@Test
	fun willHandleEmptyResultFromMetadataStore() {
		val result = emptyList<Map<String, Any>>()
		given(metadataStoreRepository.fetchUnreconciledRecords()).willReturn(result)
		reconciliationService.startReconciliation()

		verify(metadataStoreRepository, times(1)).fetchUnreconciledRecords()
	}

	@Test
	fun updatesMetadataStoreOnlyWhenFoundInHBase() {

        val result = listOf(
                mapOf("topic_name" to "incorrect", "hbase_id" to "banana", "hbase_timestamp" to Timestamp(66L)),
                mapOf("topic_name" to "table", "hbase_id" to "1", "hbase_timestamp" to Timestamp(1L))
        )

        given(metadataStoreRepository.fetchUnreconciledRecords()).willReturn(result)
        given(hbaseRepository.recordExistsInHBase("incorrect", "banana", 66L)).willReturn(false)
		given(hbaseRepository.recordExistsInHBase("table", "1", 1L)).willReturn(true)

		reconciliationService.startReconciliation()
		verify(metadataStoreRepository, times(1)).fetchUnreconciledRecords()
	}
}
