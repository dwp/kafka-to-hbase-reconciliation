package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration


import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Configuration
@ConfigurationProperties(prefix = "hbase")
data class HbaseConfiguration(
    var zookeeperParent: String? = null,
    var zookeeperQuorum: String? = null,
    var timeoutMs: String? = null,
    var clientTimeoutMs: String? = null,
    var rpcReadTimeoutMs: String? = null
) {

    companion object {
        val logger = DataworksLogger.getLogger(HbaseConfiguration::class.toString())
    }

    fun hbaseConfiguration(): org.apache.hadoop.conf.Configuration {

        val configuration = org.apache.hadoop.conf.Configuration().apply {
            set(HConstants.ZOOKEEPER_ZNODE_PARENT, zookeeperParent ?: "/hbase")
            set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum ?: "localhost")
            setInt("hbase.zookeeper.port", 2181)
            setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, timeoutMs?.toIntOrNull() ?: 1800000)
            setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, clientTimeoutMs?.toIntOrNull() ?: 3600000)
            setInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, rpcReadTimeoutMs?.toIntOrNull() ?: 1800000)
        }

        logger.info(
            "Timeout configuration",
            "scanner" to configuration.get(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD),
            "rpc" to configuration.get(HConstants.HBASE_RPC_READ_TIMEOUT_KEY),
            "client" to configuration.get(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT)
        )

        return configuration
    }

    @Bean
    fun hbaseConnection(): Connection {

        val connection = ConnectionFactory.createConnection(HBaseConfiguration.create(hbaseConfiguration()))
        addShutdownHook(connection)

        logger.info("Established connection with Hbase")

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
