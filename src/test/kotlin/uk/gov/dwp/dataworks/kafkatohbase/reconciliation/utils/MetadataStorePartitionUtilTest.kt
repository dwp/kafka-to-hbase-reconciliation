package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils


import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
class MetadataStorePartitionUtilTest {

    @Test
    fun validatePartitionsShouldReturnTrueWhenBothPartitionsAreSetAsNumbers() {

        val startPartition = "0"
        val endPartition = "16"

        val result = validatePartitions(startPartition, endPartition)

        result shouldBe true
    }

    @Test
    fun validatePartitionsShouldReturnTrueWhenBothPartitionsAreSetAsNull() {

        val startPartition = null
        val endPartition = null

        val result = validatePartitions(startPartition, endPartition)

        result shouldBe true
    }

    @Test
    fun validatePartitionsShouldReturnFalseWhenOnePartitionIsSetAsNull() {

        val startPartition = "0"
        val endPartition = null

        val result = validatePartitions(startPartition, endPartition)

        result shouldBe false
    }

    @Test
    fun validatePartitionsShouldReturnFalseWhenOnePartitionIsSetAsNOT_SET() {

        val startPartition = "0"
        val endPartition = "NOT_SET"

        val result = validatePartitions(startPartition, endPartition)

        result shouldBe false
    }

    @Test
    fun partitionIdToCsvShouldReturnCorrectlyFormattedStringWithCorrectIds() {

        val startPartition = "0"
        val endPartition = "16"

        val expectedPartitionCSV = "p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16"

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