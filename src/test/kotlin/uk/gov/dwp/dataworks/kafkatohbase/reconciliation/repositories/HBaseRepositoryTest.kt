package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import com.nhaarman.mockitokotlin2.*
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.junit4.SpringRunner
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.HBaseConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.TableNameUtil

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [HBaseRepository::class])
class HBaseRepositoryTest {

    @SpyBean
    @Autowired
    private lateinit var HBaseRepository: HBaseRepository

    @MockBean
    private lateinit var HBaseConfiguration: HBaseConfiguration

    @MockBean
    private lateinit var tableNameUtil: TableNameUtil

    @Test
    fun givenARecordExistsWhenCheckingIfItExistsFromTheTopicThenATrueResponseWillBeReturned() {

        val tableName = "ucfs_data"
        val topic = "ucfs:data"
        val id = "1"
        val version = 1L

        val adm = mock<Admin> {
            on { tableExists(TableName.valueOf(tableName)) } doReturn true
        }

        val hbaseConnection = mock<Connection> {
            on { admin } doReturn adm
        }

        whenever(HBaseConfiguration.hbaseConnection()).thenReturn(hbaseConnection)
        whenever(tableNameUtil.getTableNameFromTopic(topic)).thenReturn(tableName)
        whenever(tableNameUtil.decodePrintable("1")).thenReturn("123".toByteArray())

        val booleanResult = HBaseRepository.recordExistsInHBase(topic, id, version)

        assertThat(booleanResult).isTrue()
        verify(hbaseConnection, times(1)).close()
    }

    @Test
    fun givenARecordDoesNotExistWhenCheckingIfItExistsFromTheTopicThenAFalseResponseWillBeReturned() {

        val tableName = "ucfs_data"
        val topic = "ucfs:data"
        val id = "1"
        val version = 1L

        val adm = mock<Admin> {
            on { tableExists(TableName.valueOf(tableName)) } doReturn false
        }

        val hbaseConnection = mock<Connection> {
            on { admin } doReturn adm
        }

        whenever(HBaseConfiguration.hbaseConnection()).thenReturn(hbaseConnection)

        whenever(tableNameUtil.getTableNameFromTopic(topic)).thenReturn(tableName)
        whenever(tableNameUtil.decodePrintable("1")).thenReturn("123".toByteArray())

        val booleanResult = HBaseRepository.recordExistsInHBase(topic, id, version)

        assertThat(booleanResult).isFalse()
        verify(hbaseConnection, times(1)).close()
    }
}
