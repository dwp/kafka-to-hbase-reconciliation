package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl

import com.nhaarman.mockitokotlin2.*
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Table
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.TableNameUtil

class HBaseRepositoryImplTest {

    @Test
    fun givenARecordExistsWhenCheckingIfItExistsFromTheTopicThenATrueResponseWillBeReturned() {
        val tableNameString = "database:collection"
        val tableName = TableName.valueOf(tableNameString)
        val topic = "db.database.collection"
        val printableId = "1"
        val version = 1L
        val hbaseId = "123".toByteArray()

        val get = Get(hbaseId).apply {
            setTimeStamp(version)
        }

        val table = mock<Table> {
            on { exists(get) } doReturn true
        }

        val administration = mock<Admin> {
            on { tableExists(tableName) } doReturn true
        }

        val connection = mock<Connection> {
            on { admin } doReturn administration
            on { getTable(tableName) } doReturn table
        }

        val tableNameUtil = mock<TableNameUtil> {
            on { getTableNameFromTopic(topic) } doReturn tableNameString
            on { decodePrintable(printableId) } doReturn hbaseId
        }

        val hbaseRepository = HBaseRepositoryImpl(connection, tableNameUtil)
        assertTrue(hbaseRepository.recordExistsInHBase(topic, printableId, version))

        verify(connection, times(1)).getTable(tableName)
        verify(connection, times(1)).admin
        verifyNoMoreInteractions(connection)
        verify(administration, times(1)).tableExists(tableName)
        verifyNoMoreInteractions(administration)
        verify(table, times(1)).exists(get)
        verify(table, times(1)).close()
        verifyNoMoreInteractions(table)
    }

    @Test
    fun givenARecordDoesNotExistWhenCheckingIfItExistsFromTheTopicThenAFalseResponseWillBeReturned() {
        val tableNameString = "ucfs_data"
        val tableName = TableName.valueOf(tableNameString)
        val topic = "ucfs:data"
        val printableId = "1"
        val version = 1L
        val hbaseId = "123".toByteArray()

        val get = Get(hbaseId).apply {
            setTimeStamp(version)
        }

        val table = mock<Table> {
            on { exists(get) } doReturn false
        }

        val administration = mock<Admin> {
            on { tableExists(tableName) } doReturn true
        }

        val connection = mock<Connection> {
            on { admin } doReturn administration
            on { getTable(tableName) } doReturn table
        }

        val tableNameUtil = mock<TableNameUtil> {
            on { getTableNameFromTopic(topic) } doReturn tableNameString
            on { decodePrintable(printableId) } doReturn hbaseId
        }

        val hbaseRepository = HBaseRepositoryImpl(connection, tableNameUtil)
        assertFalse(hbaseRepository.recordExistsInHBase(topic, printableId, version))

        verify(connection, times(1)).getTable(tableName)
        verify(connection, times(1)).admin
        verifyNoMoreInteractions(connection)
        verify(administration, times(1)).tableExists(tableName)
        verifyNoMoreInteractions(administration)
        verify(table, times(1)).exists(get)
        verify(table, times(1)).close()
        verifyNoMoreInteractions(table)
    }
}
