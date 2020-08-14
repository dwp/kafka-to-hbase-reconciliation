package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.given
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.junit4.SpringRunner
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.TextUtils

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [HbaseRepository::class])
class HbaseRepositoryTest {

    @SpyBean
    @Autowired
    private lateinit var hbaseRepository: HbaseRepository

    @MockBean
    private lateinit var hbaseConnection: Connection

    @MockBean
    private lateinit var textUtils: TextUtils

    @Test
    fun recordExists() {
        val tableName: TableName = TableName.valueOf("table")
        val topic = "topic:table"
        val id = "1"
        val version = 1L

        given(hbaseConnection.getTable(tableName)).willReturn(any())

        hbaseRepository.recordExistsInHbase(topic, id, version)

    }
}