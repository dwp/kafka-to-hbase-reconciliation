package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Get
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Repository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.TableNameUtil
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Repository
@Profile("HBASE")
class HBaseRepository(private val connection: Connection, private val tableNameUtil: TableNameUtil) {

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationService::class.toString())
    }

    fun recordExistsInHBase(topicName: String, id: String, version: Long) =
            if (connection.admin.tableExists(table(topicName))) {
                (connection.getTable(table(topicName))).use {
                    it.exists(get(id, version))
                }
            } else {
                logger.warn("Table does not exist", "hbase_table_name" to (tableName(topicName) ?: ""))
                false
            }

    private fun get(id: String, version: Long) =
        Get(tableNameUtil.decodePrintable(id)).apply {
            setTimeStamp(version)
        }

    private fun table(topicName: String) = TableName.valueOf(tableName(topicName))

    private fun tableName(topicName: String) = tableNameUtil.getTableNameFromTopic(topicName)
}
