package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [CoalescedNameUtil::class])
class CoalescedNameUtilTest {

    @SpyBean
    @Autowired
    private lateinit var coalescedNameUtil: CoalescedNameUtil

    @Test
    fun givenATopicOfAgentCoreAgentToDoArchiveWhenCheckedIfCoalescedThenItShouldBeCoalesced() {
        val actual = coalescedNameUtil.coalescedName("agent_core:agentToDoArchive")
        assertThat(actual).isEqualTo("agent_core:agentToDo")
    }

    @Test
    fun givenATopicOfOtherDBAgentToDoArchiveWhenCheckedIfCoalescedThenItShouldNotBeCoalesced() {
        val actual = coalescedNameUtil.coalescedName("other_db:agentToDoArchive")
        assertThat(actual).isEqualTo("other_db:agentToDoArchive")
    }

    @Test
    fun givenATopicThatIsntAgentCoreAgentToDoArchiveWhenCheckedIfCoalescedThenItShouldNotBeCoalesced() {
        val actual = coalescedNameUtil.coalescedName("core:calculationParts")
        assertThat(actual).isEqualTo("core:calculationParts")
    }
}
