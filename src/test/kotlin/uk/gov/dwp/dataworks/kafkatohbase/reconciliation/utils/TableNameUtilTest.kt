package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils

import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import io.kotest.assertions.throwables.shouldThrow
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.junit.runner.RunWith
import org.mockito.Mockito.verifyNoInteractions
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [TableNameUtil::class])
@ActiveProfiles("HBASE")
class TableNameUtilTest {

    @SpyBean
    @Autowired
    private lateinit var tableNameUtil: TableNameUtil

    @MockBean
    private lateinit var coalescedNameUtil: CoalescedNameUtil

    @Test
    fun givenAValidTopicAndMainRegexWhenCalledToGetTableNameThenATableNameMatchedIsReturned() {

        tableNameUtil.qualifiedTablePattern = """^\w+\.([-\w]+)\.([-.\w]+)$"""

        whenever(coalescedNameUtil.coalescedName("ucfs:data")).thenReturn("ucfs_data")

        val topic = "db.ucfs.data"

        val tableName = tableNameUtil.getTableNameFromTopic(topic)

        assertThat(tableName).isEqualTo("ucfs_data")

        verify(coalescedNameUtil, times(1)).coalescedName("ucfs:data")
    }

    @Test
    fun givenAnInvalidTopicAndMainRegexWhenCalledToGetTableNameThenThereIsAnError() {

        tableNameUtil.qualifiedTablePattern = """^\w+\.([-\w]+)\.([-.\w]+)$"""

        val topic = "ucfs.data"

        val exception = shouldThrow<Exception> {
            tableNameUtil.getTableNameFromTopic(topic)
            fail("Exception not thrown when one should be for invalid topic")
        }

        assertThat(exception.message).isEqualTo("Could not derive table name from topic: '$topic', pattern: '${tableNameUtil.qualifiedTablePattern}'")

        verifyNoInteractions(coalescedNameUtil)
    }

    @Test
    fun givenAValidTopicAndEqualityRegexWhenCalledToGetTableNameThenATableNameMatchedIsReturned() {

        tableNameUtil.qualifiedTablePattern = """([-\w]+)\.([-\w]+)"""

        whenever(coalescedNameUtil.coalescedName("data:equality")).thenReturn("data_equality")

        val topic = "data.equality"

        val tableName = tableNameUtil.getTableNameFromTopic(topic)

        assertThat(tableName).isEqualTo("data_equality")

        verify(coalescedNameUtil, times(1)).coalescedName("data:equality")
    }
}
