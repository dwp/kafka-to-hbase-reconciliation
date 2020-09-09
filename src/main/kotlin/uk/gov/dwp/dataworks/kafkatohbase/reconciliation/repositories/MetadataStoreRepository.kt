package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Repository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp

@Repository
class MetadataStoreRepository(
    private val connection: Connection,
    @Qualifier("table") private val table: String,
    @Qualifier("queryLimit") private val queryLimit: String,
    @Qualifier("trimReconciledScale") private val trimReconciledScale: String,
    @Qualifier("trimReconciledUnit") private val trimReconciledUnit: String
) {

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationService::class.toString())
    }

    fun fetchUnreconciledRecords(): List<Map<String, Any>> {
        logger.debug("Fetching unreconciled records from metadata store")
        val unreconciledRecords = getUnreconciledRecordsQuery()
        return mapResultSet(unreconciledRecords)
    }

    fun reconcileRecord(topicName: String, hbaseId: String, version: Long) {
        val rowsInserted = markRecordAsReconciled(topicName, hbaseId, version)
        logger.info("Recorded processing attempt", "topic_name" to topicName, "rows_inserted" to "$rowsInserted")
    }

    private fun getUnreconciledRecordsQuery(): ResultSet? {
        val statement = connection.createStatement()
        val sql = """
                SELECT * FROM $table
                WHERE write_timestamp > CURRENT_DATE - INTERVAL 14 DAY AND 
                reconciled_result = false 
                LIMIT $queryLimit
            """.trimIndent()

        println(sql)
        return statement.executeQuery(sql)
    }

    private fun markRecordAsReconciled(topicName: String, hbaseId: String, hbaseVersion: Long) =
        with(markRecordAsReconciledStatement) {
            setString(1, topicName)
            setString(2, hbaseId)
            setTimestamp(3, Timestamp(hbaseVersion))
            executeUpdate()
        }

    private val markRecordAsReconciledStatement: PreparedStatement by lazy {
        connection.prepareStatement(
            """
                UPDATE $table
                SET reconciled_result=true, reconciled_timestamp=CURRENT_TIMESTAMP
                WHERE topic_name= ? AND hbase_id = ? and hbase_timestamp = ?
            """.trimIndent()
        )
    }

    private fun mapResultSet(resultSet: ResultSet?): MutableList<Map<String, Any>> {

        val result = listOf<Map<String, Any>>().toMutableList()

        if (resultSet == null) {
            logger.info("No records to be fetched from the metadata store")
            return result
        }

        while (resultSet.next()) {
            result.add(
                mapOf(
                    "id" to resultSet.getString("id"),
                    "hbase_id" to resultSet.getString("hbase_id"),
                    "hbase_timestamp" to resultSet.getTimestamp("hbase_timestamp"),
                    "write_timestamp" to resultSet.getString("write_timestamp"),
                    "correlation_id" to resultSet.getString("correlation_id"),
                    "topic_name" to resultSet.getString("topic_name"),
                    "kafka_partition" to resultSet.getString("kafka_partition"),
                    "kafka_offset" to resultSet.getString("kafka_offset"),
                    "reconciled_result" to resultSet.getString("reconciled_result"),
                    "reconciled_timestamp" to resultSet.getString("reconciled_timestamp")
                )
            )
        }

        logger.debug("Fetched unreconciled records from metadata store", "number_of_records" to result.size.toString())
        return result
    }

    fun deleteRecordsOlderThanPeriod(): Int {

        logger.info(
            "Deleting records in Metadata Store by scale and unit",
            "scale" to trimReconciledScale,
            "unit" to trimReconciledUnit
        )

        val statement = connection.createStatement()

        val deletedCount = statement.executeUpdate(
            """
                DELETE FROM $table
                WHERE reconciled_result = TRUE
                AND reconciled_timestamp > $trimReconciledScale $trimReconciledUnit
            """.trimIndent()
        )

        logger.info(
            "Deleted records in Metadata Store by scale and unit",
            "scale" to trimReconciledScale,
            "unit" to trimReconciledUnit,
            "deleted_count" to deletedCount.toString()
        )

        return deletedCount
    }
}
