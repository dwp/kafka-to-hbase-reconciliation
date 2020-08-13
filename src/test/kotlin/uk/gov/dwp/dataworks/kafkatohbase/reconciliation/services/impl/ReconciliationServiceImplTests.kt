package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.given
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.junit4.SpringRunner
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HbaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [ReconciliationServiceImpl::class])
class ReconciliationServiceImplTests {

	@Autowired
	private lateinit var reconciliationService: ReconciliationService

	@MockBean
	private lateinit var metadataStoreRepository: MetadataStoreRepository

	@MockBean
	private lateinit var hbaseRepository: HbaseRepository

	@Test
	fun willHandleEmptyResultFromMetadataStore() {
		val result = emptyList<Map<String, Any>>()
		given(metadataStoreRepository.fetchUnreconciledRecords()).willReturn(result)
		reconciliationService.startReconciliation()

		verify(metadataStoreRepository, times(1)).fetchUnreconciledRecords()
	}

	@Test
	fun updatesMetadataStoreOnlyWhenFoundInHbase() {

        val result = listOf(
                mapOf("topic_name" to "incorrect", "hbase_id" to "banana", "hbase_timestamp" to 66L),
                mapOf("topic_name" to "table", "hbase_id" to "1", "hbase_timestamp" to 1L)
        )

        given(metadataStoreRepository.fetchUnreconciledRecords()).willReturn(result)
        given(hbaseRepository.recordExistsInHbase("incorrect", "banana", 66L)).willReturn(false)
		given(hbaseRepository.recordExistsInHbase("table", "1", 1L)).willReturn(true)

		reconciliationService.startReconciliation()
		verify(metadataStoreRepository, times(1)).fetchUnreconciledRecords()

        // todo check the messages
	}
}
