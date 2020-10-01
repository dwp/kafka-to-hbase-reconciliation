package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.springframework.stereotype.Repository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.suppliers.ConnectionSupplier
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

@Repository
@ExperimentalTime
class MetadataStoreRepositoryImpl(private val connectionSupplier: ConnectionSupplier,
                                  private val table: String,
                                  private val numberOfParallelUpdates: Int,
                                  private val batchSize: Int): MetadataStoreRepository {

    override fun groupedUnreconciledRecords(minAgeSize: Int, minAgeUnit: String): Map<String, List<UnreconciledRecord>> =
            unreconciledRecords(minAgeSize, minAgeUnit).groupBy { it.topicName }

    override fun reconcileRecords(unreconciled: List<UnreconciledRecord>) {
        val timeTaken = measureTime {
            runBlocking {
                unreconciled.chunked(unreconciled.size / numberOfParallelUpdates).forEach { batch ->
                    launch(Dispatchers.IO) { reconcileBatch(batch) }
                }
            }
        }
        logger.info("Updated metadatastore", "record_count" to "${unreconciled.size}", "table" to table, "duration" to "$timeTaken")
    }

    private fun reconcileBatch(batch: List<UnreconciledRecord>) =
            connection().use { connection ->
                reconcileRecordStatement(connection).use { statement ->
                    try {
                        batch.forEach {
                            statement.setInt(1, it.id)
                            statement.addBatch()
                        }
                        statement.executeBatch()
                        commit(connection)
                        logger.info("Updated sub-batch", "size" to "${batch.size}")
                    } catch (e: SQLException) {
                        logger.error("Failed to update batch", e, "error" to "e.message", "error_code" to "${e.errorCode}")
                        rollback(connection)
                    }
                }
            }

    private fun unreconciledRecords(minAgeSize: Int, minAgeUnit: String): MutableList<UnreconciledRecord> =
            connection().use { connection ->
                unreconciledRecordsStatement(connection, minAgeSize, minAgeUnit).use { statement ->
                    val list = mutableListOf<UnreconciledRecord>()
                    statement.executeQuery().use { results ->
                        while (results.next()) {
                            list.add(
                                UnreconciledRecord(
                                    results.getInt("id"),
                                    results.getString("topic_name"),
                                    results.getString("hbase_id"),
                                    results.getLong("hbase_timestamp")
                                )
                            )
                        }
                    }
                    logger.info("Retrieved unreconciled records", "unreconciled_record_count" to "${list.size}", "table" to table)
                    list
                }
            }

    override fun deleteRecordsOlderThanPeriod(trimReconciledScale: String, trimReconciledUnit: String): Int {
        connection().use { connection ->
            logger.info(
                "Deleting records in Metadata Store by scale and unit",
                "scale" to trimReconciledScale,
                "unit" to trimReconciledUnit,
                "table" to table
            )

            val statement = connection.createStatement()

            val deletedCount = statement.executeUpdate(
                """
                    DELETE FROM $table
                    WHERE reconciled_result = TRUE
                    AND reconciled_timestamp < CURRENT_DATE - INTERVAL $trimReconciledScale $trimReconciledUnit
                """.trimIndent()
            )

            logger.info(
                "Deleted records in Metadata Store by scale and unit",
                "scale" to trimReconciledScale,
                "unit" to trimReconciledUnit,
                "table" to table,
                "deleted_count" to deletedCount.toString()
            )

            return deletedCount
        }
    }

    private fun unreconciledRecordsStatement(connection: Connection, minAgeSize: Int, minAgeUnit: String): PreparedStatement =
        connection.prepareStatement(
            """
                SELECT id, hbase_id, hbase_timestamp, topic_name 
                FROM $table
                WHERE write_timestamp < CURRENT_TIMESTAMP - INTERVAL $minAgeSize $minAgeUnit
                AND write_timestamp > CURRENT_DATE - INTERVAL 14 DAY
                AND reconciled_result = false
                LIMIT $batchSize
            """.trimIndent()
        )


    private fun reconcileRecordStatement(connection: Connection): PreparedStatement =
        connection.prepareStatement(
            """
                UPDATE $table
                SET reconciled_result=true, reconciled_timestamp=CURRENT_TIMESTAMP
                WHERE id = ?
            """.trimIndent()
        )

    private fun commit(connection: Connection) {
        if (!connection.autoCommit) {
            connection.commit()
        }
    }

    private fun rollback(connection: Connection) {
        if (!connection.autoCommit) {
            connection.rollback()
        }
    }

    private fun connection() = connectionSupplier.connection()

    companion object {
        private val logger = DataworksLogger.getLogger(MetadataStoreRepositoryImpl::class.java.toString())
    }
}
