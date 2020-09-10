package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.impl

import com.nhaarman.mockitokotlin2.*
import junit.framework.Assert.assertEquals
import junit.framework.Assert.assertTrue
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
            on { groupedUnreconciledRecords() } doReturn groupedRecords
        }

        val hbaseRepository = mock<HBaseRepository> {
            groupedRecords.forEach { groupedEntry ->
                on { recordsInHbase(groupedEntry.key, groupedEntry.value)} doReturn groupedEntry.value.filter { it.id % 2 == 0 }
            }
        }

        val reconciliationService = BatchedReconciliationService(hbaseRepository, metadataStoreRepository)
        reconciliationService.startReconciliation()
        verify(metadataStoreRepository, times(1)).groupedUnreconciledRecords()
        val topicCaptor = argumentCaptor<String>()
        val recordsCaptor = argumentCaptor<List<UnreconciledRecord>>()
        verify(hbaseRepository, times(10)).recordsInHbase(topicCaptor.capture(), recordsCaptor.capture())

        topicCaptor.allValues.forEachIndexed { index, topic -> assertEquals("db.database.collection${index + 1}", topic)}
        recordsCaptor.allValues.forEachIndexed { topicIndex, list ->
            list.forEachIndexed { recordIndex, unreconciledRecord ->
                assertEquals(((topicIndex + 1) * 1000) + (recordIndex + 1), unreconciledRecord.id)
                assertEquals("db.database.collection${topicIndex + 1}", unreconciledRecord.topicName)
                assertEquals("${topicIndex + 1}/${recordIndex + 1}", unreconciledRecord.hbaseId)
                assertEquals((((topicIndex + 1) * 1000) + (recordIndex + 1)).toLong(), unreconciledRecord.version)
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
