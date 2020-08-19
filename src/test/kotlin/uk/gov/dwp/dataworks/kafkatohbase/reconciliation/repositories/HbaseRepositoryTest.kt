package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import com.nhaarman.mockitokotlin2.*
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Table
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.junit4.SpringRunner
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.TableNameUtil

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [HbaseRepository::class])
class HbaseRepositoryTest {

    @SpyBean
    @Autowired
    private lateinit var hbaseRepository: HbaseRepository

    @MockBean
    private lateinit var hbaseConnection: Connection

    @MockBean
    private lateinit var tableNameUtil: TableNameUtil

    @Test
    fun givenARecordExistsWhenCheckingIfItExistsFromTheTopicThenATrueResponseWillBeReturned() {

        val tableName = "ucfs_data"
        val table = TableName.valueOf(tableName)
        val topic = "ucfs:data"
        val id = "1"
        val version = 1L

        val tableMock = mock<Table> {
            on { exists(any()) } doReturn true
        }

        whenever(tableNameUtil.getTableNameFromTopic(topic)).thenReturn(tableName)
        whenever(tableNameUtil.decodePrintable("1")).thenReturn("123".toByteArray())
        whenever(hbaseConnection.getTable(table)).thenReturn(tableMock)

        val booleanResult = hbaseRepository.recordExistsInHbase(topic, id, version)

        assertThat(booleanResult).isTrue()
        verify(tableMock, times(1)).exists(any())
    }

    @Test
    fun givenARecordDoesNotExistWhenCheckingIfItExistsFromTheTopicThenAFalseResponseWillBeReturned() {

        val tableName = "ucfs_data"
        val table = TableName.valueOf(tableName)
        val topic = "ucfs:data"
        val id = "1"
        val version = 1L

        val tableMock = mock<Table> {
            on { exists(any()) } doReturn false
        }

        whenever(tableNameUtil.getTableNameFromTopic(topic)).thenReturn(tableName)
        whenever(tableNameUtil.decodePrintable("1")).thenReturn("123".toByteArray())
        whenever(hbaseConnection.getTable(table)).thenReturn(tableMock)

        val booleanResult = hbaseRepository.recordExistsInHbase(topic, id, version)

        assertThat(booleanResult).isFalse()
        verify(tableMock, times(1)).exists(any())
    }
}
