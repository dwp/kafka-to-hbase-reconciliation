package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl

import org.springframework.stereotype.Repository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.MetadataStoreRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ScheduledReconciliationService
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.suppliers.ConnectionSupplier
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.sql.PreparedStatement
import java.sql.SQLException

@Repository
class MetadataStoreRepositoryImpl(
    private val connectionSupplier: ConnectionSupplier,
    private val table: String
) : MetadataStoreRepository {

    override fun groupedUnreconciledRecords(
        minAgeSize: Int,
        minAgeUnit: String
    ): Map<String, List<UnreconciledRecord>> =
        unreconciledRecords(minAgeSize, minAgeUnit).groupBy { it.topicName }

    override fun reconcileRecords(unreconciled: List<UnreconciledRecord>) {
        logger.info("Reconciling records", "record_count" to "${unreconciled.size}")
        if (unreconciled.isNotEmpty()) {
            logger.info("Reconciling records", "record_count" to "${unreconciled.size}")
            with(reconcileRecordStatement) {
                try {
                    unreconciled.forEach {
                        setInt(1, it.id)
                        addBatch()
                    }
                    executeBatch()
                    commit()
                } catch (e: SQLException) {
                    logger.error("Failed to update batch", e, "error" to "e.message", "error_code" to "${e.errorCode}")
                    rollback()
                }
            }
            logger.info("Reconciled records", "record_count" to "${unreconciled.size}")
        } else {
            logger.info("No records to be reconciled")
        }
    }

    private fun unreconciledRecords(minAgeSize: Int, minAgeUnit: String): MutableList<UnreconciledRecord> {
        val list = mutableListOf<UnreconciledRecord>()
        unreconciledRecordsStatement(minAgeSize, minAgeUnit).use { statement ->
            statement.executeQuery().use { results ->
                while (results.next()) {
                    list.add(
                        UnreconciledRecord(
                            results.getInt("id"),
                            results.getString("topic_name"),
                            results.getString("hbase_id"),
                            results.getTimestamp("hbase_timestamp").time
                        )
                    )
                }
            }
        }

        logger.info("Retrieved unreconciled records", "unreconciled_record_count" to "${list.size}")

        return list
    }

    override fun deleteRecordsOlderThanPeriod(trimReconciledScale: String, trimReconciledUnit: String): Int {

        logger.info(
            "Deleting records in Metadata Store by scale and unit",
            "scale" to trimReconciledScale,
            "unit" to trimReconciledUnit,
            "table" to table
        )

        val statement = connection().createStatement()

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

    private fun unreconciledRecordsStatement(minAgeSize: Int, minAgeUnit: String): PreparedStatement =
        connection().prepareStatement(
            """
                SELECT id, hbase_id, hbase_timestamp, topic_name 
                FROM $table
                WHERE write_timestamp < CURRENT_TIMESTAMP - INTERVAL $minAgeSize $minAgeUnit
                AND write_timestamp > CURRENT_DATE - INTERVAL 14 DAY
                AND reconciled_result = false
                LIMIT 100000
            """.trimIndent()
        )


    private val reconcileRecordStatement: PreparedStatement by lazy {
        connection().prepareStatement(
            """
                UPDATE $table
                SET reconciled_result=true, reconciled_timestamp=CURRENT_TIMESTAMP
                WHERE id = ?
            """.trimIndent()
        )
    }

    private fun commit() {
        if (!connection().autoCommit) {
            connection().commit()
        }
    }

    private fun rollback() {
        if (!connection().autoCommit) {
            connection().rollback()
        }
    }

    private fun connection() = connectionSupplier.connection()

    companion object {
        private val logger = DataworksLogger.getLogger(ScheduledReconciliationService::class.toString())
    }
}
