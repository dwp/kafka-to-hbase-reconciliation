package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import ch.qos.logback.core.util.OptionHelper.getEnv
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.io.File
import java.sql.DriverManager
import java.util.*

@Configuration
class Context {
    @Bean
    fun hbaseConnection(): Connection {
        val configuration = HBaseConfiguration.create().apply {
            set(HConstants.ZOOKEEPER_QUORUM, hbaseZookeeperQuorum)
            setInt("hbase.zookeeper.port", 2181)
        }

        val connection = ConnectionFactory.createConnection(configuration)
        addShutdownHook(connection)
        return connection

    }
    
    @Bean
    fun metadatastoreConnection(): java.sql.Connection {
        val hostname = metadatastoreEndpoint
        val port = metadatastorePort
        val jdbcUrl = "jdbc:mysql://$hostname:$port/"

        val properties = Properties().apply {
            put("user", metadatastoreUser)
            put("rds.password.secret.name", getEnv("K2HB_RDS_PASSWORD_SECRET_NAME") ?: "metastore_password")
            put("database", getEnv("K2HB_RDS_DATABASE_NAME") ?: "database")
            put("rds.endpoint", metadatastoreEndpoint)
            put("rds.port", metadatastorePort)
            put("use.aws.secrets", "false")

            val isUsingAWS = false  //TODO: source application
            if (isUsingAWS) {
                put("ssl_ca_path", getEnv("K2HB_RDS_CA_CERT_PATH") ?: "/certs/AmazonRootCA1.pem")
                put("ssl_ca", readFile(getProperty("ssl_ca_path")))
                put("ssl_verify_cert", true)
            }
        }

        properties["password"] = metadatastorePassword

        return DriverManager.getConnection(jdbcUrl, properties)
    }

    private fun readFile(fileName: String): String
            = File(fileName).readText(Charsets.UTF_8)

    private fun addShutdownHook(connection: Connection) {
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                println("$connection")
                connection.close()
            }
        })
    }

    @Value("\${hbase.zookeeper.quorum}")
    private lateinit var hbaseZookeeperQuorum: String

    @Value("\${metadatastore.endpoint}")
    private lateinit var metadatastoreEndpoint: String

    @Value("\${metadatastore.port}")
    private lateinit var metadatastorePort: String

    @Value("\${metadatastore.user}")
    private lateinit var metadatastoreUser: String

    @Value("\${metadatastore.password}")
    private lateinit var metadatastorePassword: String
}
