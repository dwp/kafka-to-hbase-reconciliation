package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl

import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Table
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.TableNameUtil

class HBaseRepositoryImplTest {

    @Test
    fun confirmReplicaIsSetToDefaultGivenNegativeValue() {
        val replicationFactor = -5
        assertThat(confirmConnectionToHbaseAndRecordsCanBeRetrieved(replicationFactor)).isEqualTo(-1)
    }

    @Test
    fun confirmRandomisedReplicaIdIsAboveZero() {
        val replicationFactor = 3
        assertThat(confirmConnectionToHbaseAndRecordsCanBeRetrieved(replicationFactor)).isGreaterThan(0)
    }

    @Test
    fun confirmRandomisedReplicaIdIsBelowReplicationFactor() {
        val replicationFactor = 3
        assertThat(confirmConnectionToHbaseAndRecordsCanBeRetrieved(replicationFactor)).isLessThan(3)
    }

    @Test
    fun confirmRandomisedReplicaIdWithinAcceptedRange() {
        val replicationFactor = 3
        assertThat(confirmConnectionToHbaseAndRecordsCanBeRetrieved(replicationFactor)).isBetween(1, replicationFactor - 1)
    }

    @Test
    fun confirmRandomisedReplicaIdIsHbaseDefaultIfReplicationFactorIsOne() {
        val replicationFactor = 1
        assertThat(confirmConnectionToHbaseAndRecordsCanBeRetrieved(replicationFactor)).isEqualTo(-1)
    }

    fun confirmConnectionToHbaseAndRecordsCanBeRetrieved(replicationFactor: Int) : Int {

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

        val hBaseRepository = HBaseRepositoryImpl(connection, tableNameUtil, replicationFactor)
        val unreconciled = hBaseRepository.recordsInHbase(topic, unreconciledRecords)
        assertEquals(1, getCaptor.allValues.size)
        var replicaId = 0
        getCaptor.firstValue.forEachIndexed { index, get ->
            assertEquals("${index + 1}", String(get.row))
            assertTrue(get.isCheckExistenceOnly)
            replicaId = get.replicaId
        }

        assertEquals(50, unreconciled.first.size)

        unreconciled.first.forEachIndexed { index, record ->
            assertEquals((index + 1) * 2, record.id)
        }

        return replicaId
    }
}
