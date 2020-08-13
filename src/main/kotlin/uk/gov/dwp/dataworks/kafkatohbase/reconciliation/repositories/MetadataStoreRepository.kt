package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import org.springframework.stereotype.Repository
import java.sql.Connection

@Repository
class MetadataStoreRepository(private val connection: Connection) {

    fun fetchUnreconciledRecords(): List<Map<String, Any>> {
        val result = listOf<Map<String, Any>>().toMutableList()
        val stmt = connection.createStatement()
        val resultSet = stmt.executeQuery("SELECT * FROM ucfs WHERE write_timestamp > CURRENT_DATE - INTERVAL 14 DAY AND reconciled_result = false LIMIT 100")
        while (resultSet.next()) {
            result.add(mapOf(
                    "id" to resultSet.getString("id"),
                    "hbase_id" to resultSet.getString("hbase_id"),
                    "hbase_timestamp" to resultSet.getLong("hbase_timestamp"),
                    "write_timestamp" to resultSet.getString("write_timestamp"),
                    "correlation_id" to resultSet.getString("correlation_id"),
                    "topic_name" to resultSet.getString("topic_name"),
                    "kafka_partition" to resultSet.getString("kafka_partition"),
                    "kafka_offest" to resultSet.getString("kafka_offest"),
                    "reconciled_result" to resultSet.getString("reconciled_result"),
                    "reconciled_timestamp" to resultSet.getString("reconciled_timestamp")
            ))
        }

        //TODO: verify response

        return result
    }

    fun reconcileRecord(topicName: String) {
        val stmt = connection.createStatement()
        stmt.executeQuery("UPDATE ucfs SET reconciled_result=true, reconciled_timestamp=CURRENT_TIMESTAMP WHERE topic_name=$topicName")
    }
}
