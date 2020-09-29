package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HBaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository

internal class BatchedReconciliationServiceTest {

    @Test
    fun startReconciliation() {

        val groupedRecords = (1..10).map { topicNumber ->
            (1..100).map {recordNumber ->
                UnreconciledRecord(topicNumber * 1000 + recordNumber,
                    "db.database.collection$topicNumber",
                    "$topicNumber/$recordNumber", ((topicNumber) * 1000 + (recordNumber)).toLong())
            }
        }.associateBy {it[0].topicName}

        val metadataStoreRepository = mock<MetadataStoreRepository> {
            on { groupedUnreconciledRecords(10, "MINUTE") } doReturn groupedRecords
        }

        val hbaseRepository = mock<HBaseRepository> {
            groupedRecords.forEach { groupedEntry ->
                on { recordsInHbase(groupedEntry.key, groupedEntry.value)} doReturn groupedEntry.value.filter { it.id % 2 == 0 }
            }
        }

        val reconciliationService = BatchedReconciliationService(hbaseRepository, metadataStoreRepository, 10, "MINUTE")
        reconciliationService.startReconciliation()
        verify(metadataStoreRepository, times(1)).groupedUnreconciledRecords(10, "MINUTE")
        val topicCaptor = argumentCaptor<String>()
        val recordsCaptor = argumentCaptor<List<UnreconciledRecord>>()
        verify(hbaseRepository, times(10)).recordsInHbase(topicCaptor.capture(), recordsCaptor.capture())

        //println(topicCaptor.allValues)

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
        val unreconciled = reconcileCaptor.firstValue
        val ids = unreconciled.map{ it.id }.toSortedSet()
        assertEquals(500, unreconciled.size)
        assertEquals(500, ids.size)
        unreconciled.forEach { unreconciledRecord -> assertTrue(unreconciledRecord.id % 2 == 0)}
        verifyNoMoreInteractions(metadataStoreRepository)
    }
}
