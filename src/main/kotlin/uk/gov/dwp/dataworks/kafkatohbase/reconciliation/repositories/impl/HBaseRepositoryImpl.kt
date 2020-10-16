package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.impl

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Consistency
import org.apache.hadoop.hbase.client.Get
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Repository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain.UnreconciledRecord
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.repositories.HBaseRepository
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.TableNameUtil
import uk.gov.dwp.dataworks.logging.DataworksLogger
import kotlin.random.Random

@Repository
@Profile("HBASE")
class HBaseRepositoryImpl(
    private val connection: Connection,
    private val tableNameUtil: TableNameUtil,
    private val replicationFactor: Int
) : HBaseRepository {

    override fun recordsInHbase(topicName: String, records: List<UnreconciledRecord>) =
        recordsExistInHBase(topicName, records)
            .asSequence()
            .filter { it.second }
            .map { it.first }
            .toList()

    private fun recordsExistInHBase(
        topicName: String,
        records: List<UnreconciledRecord>
    ): List<Pair<UnreconciledRecord, Boolean>> {

        val replicaId = randomiseReplicaId(replicationFactor)

        return if (records.isNotEmpty()) {
            if (tableExists(topicName)) {
                (connection.getTable(table(topicName))).use { table ->
                    val results = records.zip(
                        table.existsAll(records.map {
                            get(
                                it.hbaseId,
                                it.version,
                                replicaId
                            )
                        })
                            .asIterable()
                    )
                    val (found, notFound)
                            = results.partition(Pair<UnreconciledRecord, Boolean>::second)

                    logger.info(
                        "Checked batch of records from metadata store", "size" to "${records.size}",
                        "topic" to topicName,
                        "found" to "${found.size}", "not_found" to "${notFound.size}",
                        "replication_factor" to "${replicationFactor}",
                        "replica_id" to "${replicaId}"
                    )

                    notFound.asSequence().map { it.first }.forEach {
                        logger.debug(
                            "Record not found",
                            "topic_name" to topicName,
                            "hbase_id" to it.hbaseId,
                            "timestamp" to "${it.version}",
                            "replication_factor" to "${replicationFactor}",
                            "replica_id" to "${replicaId}"
                        )
                    }

                    results
                }
            } else {
                logger.warn(
                    "Table does not exist, marking all as not in hbase",
                    "table_name" to (tableName(topicName) ?: "")
                )
                records.map { Pair(it, false) }
            }
        } else {
            logger.info("There are no records to be reconciled in Metadata Store")
            listOf()
        }
    }


    private fun tableExists(topicName: String): Boolean {
        return if (_mutableMap.containsKey(topicName)) {
            true
        } else {
            val exists = connection.admin.tableExists(table(topicName))
            if (exists) {
                _mutableMap[topicName] = true
            }
            exists
        }
    }

    private var _mutableMap: MutableMap<String, Boolean> = mutableMapOf()

    private fun get(id: String, version: Long, replicaId: Int) = Get(tableNameUtil.decodePrintable(id)).apply {
        setTimeStamp(version)
        isCheckExistenceOnly = true
        consistency = Consistency.TIMELINE
        setReplicaId(replicaId)
    }

    private fun table(topicName: String) = TableName.valueOf(tableName(topicName))
    private fun tableName(topicName: String) = tableNameUtil.getTableNameFromTopic(topicName)

    fun randomiseReplicaId(replicationFactor: Int): Int {
        val start = 1
        val end = replicationFactor - 1
        val hbaseDefault = -1

        if (end <= start) {
            return hbaseDefault
        }
        return Random(System.nanoTime()).nextInt(start, end)
    }

    companion object {
        val logger = DataworksLogger.getLogger(HBaseRepositoryImpl::class.toString())
    }
}
