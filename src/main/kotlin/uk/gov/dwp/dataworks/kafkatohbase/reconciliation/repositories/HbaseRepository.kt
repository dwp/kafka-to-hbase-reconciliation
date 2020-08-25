package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Get
import org.springframework.stereotype.Repository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.TableNameUtil
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Repository
class HbaseRepository(private val hbaseConnection: Connection,
                      private val tableNameUtil: TableNameUtil) {

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationService::class.toString())
    }

    fun recordExistsInHbase(topicName: String, id: String, version: Long): Boolean {

        val table = TableName.valueOf(tableNameUtil.getTableNameFromTopic(topicName))
        val decodedId = Get(tableNameUtil.decodePrintable(id))

        logger.info("Verifying that record exists within the table",
                "table" to table.nameAsString,
                "decoded_id" to decodedId.id)

        return hbaseConnection.getTable(table).exists(decodedId)
    }
}
