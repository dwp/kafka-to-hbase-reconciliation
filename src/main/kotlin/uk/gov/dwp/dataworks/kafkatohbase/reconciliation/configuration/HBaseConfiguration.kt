package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Configuration
@ConfigurationProperties(prefix = "hbase")
@Profile("HBASE")
data class HBaseConfiguration(
    var zookeeperParent: String? = "NOT_SET",
    var zookeeperQuorum: String? = "NOT_SET",
    var zookeeperPort: String? = "NOT_SET",
    var clientScannerTimeoutPeriodMs: String? = "NOT_SET",
    var clientOperationTimeoutMs: String? = "NOT_SET",
    var rpcReadTimeoutMs: String? = "NOT_SET",
    var retries: String? = "NOT_SET",
    var replicationFactor: String? = "NOT_SET"
) {

    companion object {
        val logger = DataworksLogger.getLogger(HBaseConfiguration::class.toString())
    }

    fun hbaseConfiguration(): org.apache.hadoop.conf.Configuration {

        val configuration = org.apache.hadoop.conf.Configuration().apply {
            set(HConstants.ZOOKEEPER_ZNODE_PARENT, zookeeperParent ?: "NOPE")
            set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum ?: "NOPE")
            setInt("hbase.zookeeper.port", zookeeperPort?.toIntOrNull() ?: 666)
            setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, clientScannerTimeoutPeriodMs?.toIntOrNull() ?: 666)
            setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, clientOperationTimeoutMs?.toIntOrNull() ?: 666)
            setInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, rpcReadTimeoutMs?.toIntOrNull() ?: 666)
            setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, retries?.toIntOrNull() ?: 666)
        }



        logger.info("Timeout configuration",
            "scanner" to configuration.get(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD),
            "rpc" to configuration.get(HConstants.HBASE_RPC_READ_TIMEOUT_KEY),
            "client" to configuration.get(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT))

        logger.info("HBase Configuration loaded", "hbase_configuration" to configuration.toString())
        return configuration
    }

    @Bean
    fun hbaseConnection(): Connection {
        logger.info("Establishing connection with HBase")
        val configuration = hbaseConfiguration()
        logger.info("Hbase connection configuration",
            HConstants.ZOOKEEPER_ZNODE_PARENT to configuration.get(HConstants.ZOOKEEPER_ZNODE_PARENT),
            HConstants.ZOOKEEPER_QUORUM to configuration.get(HConstants.ZOOKEEPER_QUORUM),
            "hbase.zookeeper.port" to configuration.get("hbase.zookeeper.port"))
        val connection = ConnectionFactory.createConnection(HBaseConfiguration.create(configuration))
        addShutdownHook(connection)
        logger.info("Established connection with HBase")

        return connection
    }

    @Bean
    fun replicationFactor() = replicationFactor?.toIntOrNull() ?: 1

    private fun addShutdownHook(connection: Connection) {
        logger.info("Adding HBase shutdown hook")
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                logger.info("HBase shutdown hook running - closing connection")
                connection.close()
            }
        })
        logger.info("Added HBase shutdown hook")
    }
}
