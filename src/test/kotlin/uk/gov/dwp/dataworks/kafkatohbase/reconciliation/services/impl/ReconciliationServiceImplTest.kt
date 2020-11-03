package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HBaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import kotlin.time.ExperimentalTime

@ExperimentalTime
internal class ReconciliationServiceImplTest {

    @Test
    fun startReconciliation() {

        val recordsInHBase = (1..10).map { topicNumber ->
            (1..100).map {recordNumber ->
                UnreconciledRecord(topicNumber * 1000 + recordNumber,
                    "db.database.collection$topicNumber",
                    "$topicNumber/$recordNumber", ((topicNumber) * 1000 + (recordNumber)).toLong())
            }
        }.associateBy {it[0].topicName}


        val metadataStoreRepository = mock<MetadataStoreRepository> {
            on { groupedUnreconciledRecords(10, "MINUTE", 5, "HOUR") } doReturn recordsInHBase
        }

        val hbaseRepository = mock<HBaseRepository> {
            recordsInHBase.forEach { groupedEntry ->
                on { recordsInHbase(groupedEntry.key, groupedEntry.value)} doReturn groupedEntry.value.partition { it.id % 2 == 0 }
            }
        }

        val reconciliationService = ReconciliationServiceImpl(hbaseRepository, metadataStoreRepository,
            10, "MINUTE", 5, "HOUR")
        reconciliationService.start()
        verify(metadataStoreRepository, times(1)).groupedUnreconciledRecords(10, "MINUTE", 5, "HOUR")
        val topicCaptor = argumentCaptor<String>()
        val recordsCaptor = argumentCaptor<List<UnreconciledRecord>>()
        verify(hbaseRepository, times(10)).recordsInHbase(topicCaptor.capture(), recordsCaptor.capture())

        for (topicIndex in 1 .. 10) {
            assertTrue(topicCaptor.allValues.contains("db.database.collection${topicIndex}"))
        }

        recordsCaptor.allValues.forEach { list ->
            list.forEachIndexed { recordIndex, unreconciledRecord ->
                val matchResult = Regex("""\d+$""").find(unreconciledRecord.topicName)
                val topicNo = matchResult?.groupValues?.get(0)?.toInt()!!
                assertEquals(((topicNo) * 1000) + (recordIndex + 1), unreconciledRecord.id)
                assertEquals("db.database.collection${topicNo}", unreconciledRecord.topicName)
                assertEquals("${topicNo}/${recordIndex + 1}", unreconciledRecord.hbaseId)
                assertEquals((((topicNo) * 1000) + (recordIndex + 1)).toLong(), unreconciledRecord.version)
            }
        }
        verifyNoMoreInteractions(hbaseRepository)

        val reconcileCaptor = argumentCaptor<List<UnreconciledRecord>>()
        verify(metadataStoreRepository, times(1)).reconcileRecords(reconcileCaptor.capture())
        val inHBase = reconcileCaptor.firstValue
        val presentIds = inHBase.map{ it.id }.toSortedSet()
        assertEquals(500, inHBase.size)
        assertEquals(500, presentIds.size)
        inHBase.forEach { unreconciledRecord -> assertTrue(unreconciledRecord.id % 2 == 0)}

        val lastCheckedCaptor = argumentCaptor<List<UnreconciledRecord>>()
        verify(metadataStoreRepository, times(1)).recordLastChecked(lastCheckedCaptor.capture())
        val notInHbase = lastCheckedCaptor.firstValue
        val notPresentIds = notInHbase.map(UnreconciledRecord::id).toSortedSet()
        assertEquals(500, notInHbase.size)
        assertEquals(500, notPresentIds.size)
        notInHbase.forEach { record ->  assertTrue(record.id % 2 != 0)}
        verifyNoMoreInteractions(metadataStoreRepository)
    }
}
