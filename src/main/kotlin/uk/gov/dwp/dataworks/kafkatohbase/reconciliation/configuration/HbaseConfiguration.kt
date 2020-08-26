package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary

@org.springframework.context.annotation.Configuration
@ConfigurationProperties(prefix = "hbase")
data class HbaseConfiguration(
    var zookeeperParent: String? = null,
    var zookeeperQuorum: String? = null,
    var zookeeperPort: String? = null,
    var rpcTimeoutMilliseconds: String? = null,
    var operationTimeoutMilliseconds: String? = null,
    var pauseMilliseconds: String? = null,
    var retries: String? = null
) {

    fun hbaseConfiguration(): Configuration = HBaseConfiguration.create().apply {
        set(HConstants.ZOOKEEPER_ZNODE_PARENT, zookeeperParent ?: "/hbase")
        set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum ?: "localhost")
        setInt("hbase.zookeeper.port", zookeeperPort?.toIntOrNull() ?: 2181)
        set("hbase.rpc.timeout", rpcTimeoutMilliseconds ?: "1200000")
        set("hbase.client.operation.timeout", operationTimeoutMilliseconds ?: "1800000")
        set("hbase.client.pause", pauseMilliseconds ?: "50")
        set("hbase.client.retries.number", retries ?: "3")
    }

    @Primary
    @Bean
    fun hbaseConnection(): Connection {

        val connection = ConnectionFactory.createConnection(hbaseConfiguration())
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
