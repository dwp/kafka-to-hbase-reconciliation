package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl

import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Table
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.TableNameUtil

class HBaseRepositoryImplTest {

    @Test
    fun confirmConnectionToHbaseAndRecordsCanBeRetrievedWithZeroReplicaId() {
        confirmConnectionToHbaseAndRecordsCanBeRetrieved(0,0)
    }

    @Test
    fun confirmConnectionToHbaseAndRecordsCanBeRetrievedWithReplica() {
        confirmConnectionToHbaseAndRecordsCanBeRetrieved(1,1)
    }

    @Test
    fun confirmReplicaIsSetToDefaultGivenNegativeValue() {
        confirmConnectionToHbaseAndRecordsCanBeRetrieved(-1,-2)
    }

    fun confirmConnectionToHbaseAndRecordsCanBeRetrieved(replicaIdExpected: Int, replicaIdActual: Int) {
        val topic = "db.database.collection"
        val tableName = "database:collection"

        val unreconciledRecords = (1..100).map {
            UnreconciledRecord(it, "db.database.collection", "$it", (it % 10).toLong())
        }

        val adm  = mock<Admin> {
            on { tableExists(TableName.valueOf(tableName)) } doReturn true
        }

        val existsArray = (1..100).map {it % 2 == 0}.toBooleanArray()

        val getCaptor = argumentCaptor<List<Get>>()
        val table = mock<Table> {
            on { existsAll(getCaptor.capture()) } doReturn existsArray
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(tableName)) } doReturn table
        }

        val tableNameUtil = mock<TableNameUtil> {
            on { getTableNameFromTopic(topic) } doReturn "database:collection"
            for (i in 1 .. 100) {
                on { decodePrintable("$i") } doReturn "$i".toByteArray()
            }
        }

        val hBaseRepository = HBaseRepositoryImpl(connection, tableNameUtil, replicaIdActual)
        val unreconciled = hBaseRepository.recordsInHbase(topic, unreconciledRecords)
        assertEquals(1, getCaptor.allValues.size)
        getCaptor.firstValue.forEachIndexed { index, get ->
            assertEquals("${index + 1}", String(get.row))
            assertTrue(get.isCheckExistenceOnly)
            assertEquals(replicaIdExpected, get.replicaId)
        }

        assertEquals(50, unreconciled.size)

        unreconciled.forEachIndexed { index, record ->
            assertEquals((index + 1) * 2, record.id)
        }
    }
}
