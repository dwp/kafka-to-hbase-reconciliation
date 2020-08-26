package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean

@org.springframework.context.annotation.Configuration
@ConfigurationProperties(prefix = "hbase")
data class HbaseConfiguration(
    var zookeeperParent: String? = null,
    var zookeeperQuorum: String? = null,
    var zookeeperPort: String? = null,
    var clientScannerTimeoutPeriod: String? = null,
    var clientScannerCaching: String? = null

) {

    fun hbaseConfiguration(): Configuration = Configuration().apply {
        set(HConstants.ZOOKEEPER_ZNODE_PARENT, zookeeperParent ?: "/hbase")
        set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum ?: "localhost")
        setInt("hbase.zookeeper.port", zookeeperPort?.toIntOrNull() ?: 2181)
        setInt("hbase.client.scanner.timeout.period", clientScannerTimeoutPeriod?.toIntOrNull() ?: 60000)
        setInt("hbase.client.scanner.caching", clientScannerCaching?.toIntOrNull() ?: 1)
    }

    @Bean
    fun hbaseConnection(): Connection {

        val connection = ConnectionFactory.createConnection(HBaseConfiguration.create(hbaseConfiguration()))
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
