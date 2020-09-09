package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Get
import org.springframework.stereotype.Repository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HBaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ScheduledReconciliationService
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.TableNameUtil
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Repository
class HBaseRepositoryImpl(private val connection: Connection, private val tableNameUtil: TableNameUtil):
    HBaseRepository {

    override fun recordsInHbase(topicName: String, records: List<UnreconciledRecord>) =
        recordsExistInHBase(topicName, records)
            .asSequence()
            .filter { it.second }
            .map { it.first }
            .toList()

    override fun recordExistsInHBase(topicName: String, id: String, version: Long) =
        if (connection.admin.tableExists(table(topicName))) {
            (connection.getTable(table(topicName))).use {
                it.exists(get(id, version))
            }
        } else {
            logger.warn("Table does not exist", "hbase_table_name" to (tableName(topicName) ?: ""))
            false
        }

    private fun recordsExistInHBase(topicName: String, records: List<UnreconciledRecord>): List<Pair<UnreconciledRecord, Boolean>> =
        if (connection.admin.tableExists(table(topicName))) {
            (connection.getTable(table(topicName))).use { table ->
                records.zip(table.existsAll(records.map {get(it.hbaseId, it.version)}).asIterable())
            }
        } else {
            logger.warn("Table does not exist, marking all as not in hbase", "table_name" to (tableName(topicName) ?: ""))
            records.map { Pair(it, false) }
        }

    private fun get(id: String, version: Long) = Get(tableNameUtil.decodePrintable(id)).apply { setTimeStamp(version) }
    private fun table(topicName: String) = TableName.valueOf(tableName(topicName))
    private fun tableName(topicName: String) = tableNameUtil.getTableNameFromTopic(topicName)

    companion object {
        val logger = DataworksLogger.getLogger(ScheduledReconciliationService::class.toString())
    }
}
