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
        var zookeeperQuorum: String? = null,
        @DefaultValue("NOT_SET") var tablePattern: String) {

    @Bean
    fun qualifiedTablePattern() = tablePattern

    @Bean
    fun hbaseConnection(): Connection {
        val configuration = HBaseConfiguration.create().apply {
            set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
            setInt("hbase.zookeeper.port", 2181)
        }

        val connection = ConnectionFactory.createConnection(configuration)
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
