package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils

import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.MetadataStoreConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.exceptions.MetadataStorePartitionsNotSetException


fun validatePartitions(startPartition: String?, endPartition: String?): Boolean {

    MetadataStoreConfiguration.logger.info("Validating metadata store partitions",
            "start_partition" to (startPartition ?: "NOT_SET"),
            "end_partition" to (endPartition ?: "NOT_SET"))

    val startPartitionSet = startPartition == null || startPartition == "NOT_SET"
    val endPartitionSet = endPartition == null || endPartition == "NOT_SET"

    val isValid = startPartitionSet == endPartitionSet
    if (!isValid) {
        throw MetadataStorePartitionsNotSetException("Both partitions need to be set to make use of partitioning functionality")
    }
    return isValid
}

fun toPartitionIdCSV(startPartition: String, endPartition: String) =
        if (startPartition == "NOT_SET" || endPartition == "NOT_SET") {
            "NOT_SET"
        } else {
            val startPartitionInt = startPartition.toInt()
            val endPartitionInt = endPartition.toInt()
            var partitionCSV = ""

            for (i in startPartitionInt..endPartitionInt) {
                partitionCSV += "p$i"
                if (i != endPartitionInt) {
                    partitionCSV += ","
                }
            }
            partitionCSV
        }