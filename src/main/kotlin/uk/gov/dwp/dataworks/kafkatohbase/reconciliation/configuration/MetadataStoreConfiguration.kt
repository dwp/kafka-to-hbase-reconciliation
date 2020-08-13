package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import ch.qos.logback.core.util.OptionHelper
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.readFile
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component
import java.sql.Connection
import java.sql.DriverManager
import java.util.*

@Component
@ConfigurationProperties(prefix = "metadatastore")
@EnableConfigurationProperties
data class MetadataStoreConfiguration(
        var endpoint: String? = null,
        var port: String? = null,
        var user: String? = null,
        var password: String? = null) {

    @Bean
    fun metadataStoreConnection(): Connection {
        val hostname = endpoint
        val port = port
        val jdbcUrl = "jdbc:mysql://$hostname:$port/"

        val properties = Properties().apply {
            put("user", user)
            put("rds.password.secret.name", OptionHelper.getEnv("K2HB_RDS_PASSWORD_SECRET_NAME") ?: "metastore_password")
            put("database", OptionHelper.getEnv("K2HB_RDS_DATABASE_NAME") ?: "database")
            put("rds.endpoint", endpoint)
            put("rds.port", port)
            put("use.aws.secrets", "false")

            val isUsingAWS = false  //TODO: source application
            if (isUsingAWS) {
                put("ssl_ca_path", OptionHelper.getEnv("K2HB_RDS_CA_CERT_PATH") ?: "/certs/AmazonRootCA1.pem")
                put("ssl_ca", readFile(getProperty("ssl_ca_path")))
                put("ssl_verify_cert", true)
            }
        }

        properties["password"] = password

        return DriverManager.getConnection(jdbcUrl, properties)
    }
}
