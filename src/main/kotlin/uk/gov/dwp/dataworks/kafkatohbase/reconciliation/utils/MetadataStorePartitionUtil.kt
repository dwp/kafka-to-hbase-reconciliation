package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils

fun toPartitionIdCSV(startPartition: String, endPartition: String) =
        if (startPartition == "NOT_SET" || endPartition == "NOT_SET") {
            "NOT_SET"
        } else {
            val startPartitionInt = startPartition.toInt()
            val endPartitionInt = endPartition.toInt()
            var partitionCSV = ""

            for (i in startPartitionInt..endPartitionInt) {
                partitionCSV += i.toString()
                if (i != endPartitionInt) {
                    partitionCSV += ","
                }
            }
            partitionCSV
        }