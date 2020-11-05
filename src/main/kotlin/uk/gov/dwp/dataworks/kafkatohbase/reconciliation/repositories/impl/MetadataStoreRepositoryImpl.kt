package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.springframework.stereotype.Repository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.suppliers.ConnectionSupplier
import uk.gov.dwp.dataworks.logging.DataworksLogger
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.exceptions.OptimiseTableFailedException
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Statement
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

@Repository
@ExperimentalTime
class MetadataStoreRepositoryImpl(private val connectionSupplier: ConnectionSupplier,
                                  private val table: String,
                                  private val numberOfParallelUpdates: Int,
                                  private val batchSize: Int,
                                  private val deleteLimit: Int,
                                  private val partitions: String) : MetadataStoreRepository {

    override fun groupedUnreconciledRecords(minAgeSize: Int, minAgeUnit: String,
                                            lastCheckedScale: Int, lastCheckedUnit: String): Map<String, List<UnreconciledRecord>> =
            unreconciledRecords(minAgeSize, minAgeUnit, lastCheckedScale, lastCheckedUnit).groupBy { it.topicName }

    override fun reconcileRecords(unreconciled: List<UnreconciledRecord>) {
        if (unreconciled.isNotEmpty()) {
            val timeTaken = measureTime {
                runBlocking {
                    unreconciled.chunked(maxOf(unreconciled.size / numberOfParallelUpdates, 1)).forEach { batch ->
                        launch(Dispatchers.IO) {
                            reconcileBatch(batch)
                        }
                    }
                }
            }
            logger.info("Updated metadatastore", "record_count" to "${unreconciled.size}", "table" to table, "duration" to "$timeTaken")
        }
    }

    private fun reconcileBatch(batch: List<UnreconciledRecord>) =
            connection().updateBatch(batch) {
                reconcileRecordStatement(it)
            }

    override fun recordLastChecked(batch: List<UnreconciledRecord>) =
            connection().updateBatch(batch) { recordLastCheckedStatement(it) }

    fun Connection.updateBatch(batch: List<UnreconciledRecord>, prepare: (Connection) -> PreparedStatement) =
            use { prepare(it).updateBatchById(batch) }

    private fun PreparedStatement.updateBatchById(batch: List<UnreconciledRecord>) {
        use {
            try {
                batch.forEach {
                    setInt(1, it.id)
                    addBatch()
                }
                executeBatch()
                commit(connection)
                logger.info("Updated batch", "size" to "${batch.size}")
            } catch (e: SQLException) {
                logger.error("Failed to update batch", e, "error" to "${e.message}", "error_code" to "${e.errorCode}")
                e.printStackTrace(System.err)
                rollback(connection)
            }
        }
    }

    private fun unreconciledRecords(minAgeSize: Int, minAgeUnit: String,
                                    lastCheckedScale: Int, lastCheckedUnit: String): MutableList<UnreconciledRecord> =
            connection().use { connection ->
                val unreconciledStatement = if (partitionsSet(partitions)) {
                    logger.info("Getting unreconciled records from partitioned table", "table" to table, "partitions" to partitions)
                    unreconciledRecordsStatementPartitioned(connection, minAgeSize, minAgeUnit, lastCheckedScale, lastCheckedUnit, partitions)
                } else {
                    logger.info("Getting unreconciled records from partitioned table", "table" to table)
                    unreconciledRecordsStatement(connection, minAgeSize, minAgeUnit, lastCheckedScale, lastCheckedUnit)
                }

                unreconciledStatement.use { statement ->
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

    override fun deleteRecordsOlderThanPeriod(trimReconciledScale: String, trimReconciledUnit: String): Int =
            connection().use { connection ->
                logger.info("Deleting records in Metadata Store by scale and unit",
                        "scale" to trimReconciledScale,
                        "unit" to trimReconciledUnit,
                        "table" to table)

                connection.createStatement().use {
                    val deletedCount = it.executeUpdate("""
                    DELETE FROM $table
                    WHERE reconciled_result = TRUE
                    AND reconciled_timestamp < CURRENT_DATE - INTERVAL $trimReconciledScale $trimReconciledUnit
                """.trimIndent())

                    logger.info("Deleted records in Metadata Store by scale and unit",
                            "scale" to trimReconciledScale,
                            "unit" to trimReconciledUnit,
                            "table" to table,
                            "deleted_count" to deletedCount.toString())

                    deletedCount
                }

            }

    override fun deleteAllReconciledRecords(deletedAccumulation: Int): Int {
        val deletedCount = connection().use { connection ->
            connection.createStatement().use {
                val (deletedCount, duration) = measureTimedValue {
                    it.executeUpdate(
                            """
                        DELETE FROM $table
                        WHERE reconciled_result = TRUE 
                        LIMIT $deleteLimit
                    """.trimIndent()
                    )
                }

                logger.info(
                        "Deleted records in Metadata Store", "table" to table,
                        "deleted_count" to "$deletedCount", "time_taken" to "$duration", "delete_limit" to "$deleteLimit",
                        "deleted_accumulation" to "${deletedAccumulation + deletedCount}"
                )

                if (!connection.autoCommit) {
                    connection.commit()
                }

                deletedCount
            }
        }

        if (deletedCount < deleteLimit) {
            return deletedAccumulation + deletedCount
        }

        return deleteAllReconciledRecords(deletedAccumulation + deletedCount)
    }

    override fun optimizeTable(): Boolean =
        connection().use { connection ->
            try {
                connection.createStatement().use { statement ->
                    val (result, duration) = measureTimedValue {
                        statement.execute("OPTIMIZE TABLE $table")
                    }
                    logger.info(
                        "Optimized table", "table" to table,
                        "result" to "$result", "time_taken" to "$duration"
                    )
                    result
                }
            } catch (e: Exception) {
                logger.error("Optimisation of table failed", "table" to table, "exception" to "$e")
                throw OptimiseTableFailedException("Failed to optimise table: $table")
            }
        }


    private fun unreconciledRecordsStatement(connection: Connection, minAgeSize: Int, minAgeUnit: String,
                                             lastCheckedScale: Int, lastCheckedUnit: String): PreparedStatement =
            connection.prepareStatement("""SELECT id, hbase_id, hbase_timestamp, topic_name
                                        FROM $table
                                        WHERE reconciled_result = false
                                        AND write_timestamp < CURRENT_TIMESTAMP - INTERVAL $minAgeSize $minAgeUnit
                                        AND write_timestamp > CURRENT_DATE - INTERVAL 14 DAY
                                        AND (last_checked_timestamp IS NULL
                                                OR last_checked_timestamp < CURRENT_TIMESTAMP - INTERVAL $lastCheckedScale $lastCheckedUnit)
                                        LIMIT $batchSize
                                        """.trimIndent())

    private fun unreconciledRecordsStatementPartitioned(connection: Connection, minAgeSize: Int, minAgeUnit: String,
                                                        lastCheckedScale: Int, lastCheckedUnit: String, partitions: String): PreparedStatement =
            connection.prepareStatement("""SELECT id, hbase_id, hbase_timestamp, topic_name
                                        FROM $table PARTITION ($partitions)
                                        WHERE reconciled_result = false
                                        AND write_timestamp < CURRENT_TIMESTAMP - INTERVAL $minAgeSize $minAgeUnit
                                        AND write_timestamp > CURRENT_DATE - INTERVAL 14 DAY
                                        AND (last_checked_timestamp IS NULL
                                                OR last_checked_timestamp < CURRENT_TIMESTAMP - INTERVAL $lastCheckedScale $lastCheckedUnit)
                                        LIMIT $batchSize
                                        """.trimIndent())

    private fun reconcileRecordStatement(connection: Connection): PreparedStatement =
            connection.prepareStatement(
                    """
                UPDATE $table
                SET reconciled_result=true, reconciled_timestamp=CURRENT_TIMESTAMP, last_checked_timestamp=CURRENT_TIMESTAMP
                WHERE id = ?
            """.trimIndent()
            )

    private fun recordLastCheckedStatement(connection: Connection): PreparedStatement =
            connection.prepareStatement(
                    """
                UPDATE $table
                SET last_checked_timestamp=CURRENT_TIMESTAMP, last_checked_timestamp=CURRENT_TIMESTAMP
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

    fun partitionsSet(partitions: String) = partitions != "NOT_SET"

    private fun connection() = connectionSupplier.connection()

    companion object {
        private val logger = DataworksLogger.getLogger(MetadataStoreRepositoryImpl::class.java.toString())
    }
}
