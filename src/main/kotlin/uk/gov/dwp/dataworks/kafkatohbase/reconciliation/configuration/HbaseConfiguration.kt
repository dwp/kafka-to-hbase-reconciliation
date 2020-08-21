package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.context.properties.bind.DefaultValue
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "hbase")
@EnableConfigurationProperties
class HbaseConfiguration(
    var zookeeperParent: String? = null,
    var zookeeperQuorum: String? = null,
    var zookeeperPort: String? = null,
    var rpcTimeoutMilliseconds: String? = null,
    var operationTimeoutMilliseconds: String? = null,
    var pauseMilliseconds: String? = null,
    var retries: String? = null,
    @DefaultValue("NOT_SET") var qualifiedTablePattern: String,
    var columnFamily: String? = null,
    var columnQualifier: String? = null,
    var regionReplication: String? = null
) {

    @Bean
    fun qualifiedTablePattern() = qualifiedTablePattern

    @Bean
    fun hbaseConnection(): Connection {
        val configuration = HBaseConfiguration.create().apply {
            set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
            setInt("hbase.zookeeper.port", 2181)
            set("zookeeper.znode.parent", zookeeperParent ?: "/hbase")
            set("hbase.zookeeper.quorum", zookeeperQuorum ?: "zookeeper")
            setInt("hbase.zookeeper.port", zookeeperPort?.toIntOrNull() ?: 2181)
            set("hbase.rpc.timeout", rpcTimeoutMilliseconds ?: "1200000")
            set("hbase.client.operation.timeout", operationTimeoutMilliseconds ?: "1800000")
            set("hbase.client.pause", pauseMilliseconds ?: "50")
            set("hbase.client.retries.number", retries ?: "50")
        }

        val connection = ConnectionFactory.createConnection(HBaseConfiguration.create(configuration))
        addShutdownHook(connection)

        return connection
    }

    private fun addShutdownHook(connection: Connection) {
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                connection.close()
            }
        })
    }
}
