package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.springframework.stereotype.Repository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.HbaseConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.TableNameUtil
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Repository
class HbaseRepository(
    private val configuration: HbaseConfiguration,
    private val tableNameUtil: TableNameUtil
) {

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationService::class.toString())
    }

    fun recordExistsInHbase(topicName: String, id: String, version: Long): Boolean {
        configuration.hbaseConnection().use { connection ->
            with(TableName.valueOf(tableNameUtil.getTableNameFromTopic(topicName))) {
                return if (connection.admin.tableExists(this)) {
                    val decodedId = Get(tableNameUtil.decodePrintable(id))
                    logger.info(
                        "Verifying that record exists within the table",
                        "table" to this.nameAsString,
                        "decoded_id" to decodedId.id
                    )
                    true
                } else {
                    logger.error("Table does not exist", "hbase_table_name" to this.nameAsString)
                    false
                }
            }
        }
    }
}
