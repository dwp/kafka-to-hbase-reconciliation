package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import org.springframework.stereotype.Repository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.MetadataStoreConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.sql.Connection
import java.sql.ResultSet

@Repository
class MetadataStoreRepository(private val configuration: MetadataStoreConfiguration) {

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationService::class.toString())
    }

    fun fetchUnreconciledRecords(): List<Map<String, Any>> {
        logger.info("Fetching unreconciled records from metadata store")
        val unreconciledRecords = getUnreconciledRecordsQuery()
        return mapResultSet(unreconciledRecords)
    }

    fun reconcileRecord(topicName: String) {
        val rowsInserted = updateUnreconciledRecordsQuery(topicName)
        logger.info("Recorded processing attempt", "topic_name" to topicName, "rows_inserted" to "$rowsInserted")
    }

    private fun getUnreconciledRecordsQuery(): ResultSet? {
        val connection = configuration.metadataStoreConnection()
        val statement = connection.createStatement()
        return statement.executeQuery(
            """
                SELECT * FROM ${configuration.table} 
                WHERE write_timestamp > CURRENT_DATE - INTERVAL 14 DAY AND 
                reconciled_result = false 
                LIMIT 14
            """.trimIndent()
        )
    }

    private fun updateUnreconciledRecordsQuery(topicName: String): Int {
        val connection = configuration.metadataStoreConnection()
        val statement = connection.createStatement()
        return statement.executeUpdate(
            """
                UPDATE ${configuration.table} 
                SET reconciled_result=true, reconciled_timestamp=CURRENT_TIMESTAMP 
                WHERE topic_name=${topicName}
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
                    "hbase_timestamp" to resultSet.getLong("hbase_timestamp"),
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

        logger.info("Fetched unreconciled records from metadata store", "number_of_records" to result.size.toString())
        return result
    }
}
