package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils


import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
class MetadataStorePartitionUtilTest {

    @Test
    fun partitionIdToCsvShouldReturnCorrectlyFormattedStringWithCorrectIds() {

        val startPartition = "0"
        val endPartition = "16"

        val expectedPartitionCSV = "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16"

        val result = toPartitionIdCSV(startPartition, endPartition)

        result shouldBe expectedPartitionCSV
    }

    @Test
    fun partitionIdToCsvShouldReturnNotSetWhenEitherTheStartOrEndPartitionAreOfThatValue() {

        val startPartition = "0"
        val endPartition = "NOT_SET"

        val expectedPartitionCSV = "NOT_SET"

        val result = toPartitionIdCSV(startPartition, endPartition)

        result shouldBe expectedPartitionCSV
    }
}