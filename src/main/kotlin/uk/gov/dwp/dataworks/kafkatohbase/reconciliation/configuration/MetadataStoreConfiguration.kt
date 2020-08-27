package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.secrets.AWSSecretHelper
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.readFile
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.sql.Connection
import java.sql.DriverManager
import java.util.*

@Configuration
@ConfigurationProperties(prefix = "metadatastore")
data class MetadataStoreConfiguration(
    var endpoint: String? = "127.0.0.1",
    var port: String? = "3306",
    var user: String? = "reconciliationwriter",
    var passwordSecretName: String? = "metastore_password",
    var passwordDummy: String? = "password",
    var table: String? = null,
    var databaseName: String? = "metadatastore",
    var caCertPath: String? = "/certs/AmazonRootCA1.pem",
    var queryLimit: String? = "14",
    var useAwsSecrets: String? = "false"
) {

    companion object {
        val logger = DataworksLogger.getLogger(MetadataStoreConfiguration::class.toString())
    }

    private val isUsingAWS = useAwsSecrets == "true"

    fun databaseUrl() = "jdbc:mysql://$endpoint:$port/$databaseName"

    fun databaseProperties(): Properties {

        val properties = Properties().apply {
            put("user", user)

            if (isUsingAWS) {
                put("ssl_ca_path", caCertPath)
                put("ssl_ca", readFile(getProperty("ssl_ca_path")))
                put("ssl_verify_cert", true)
            }
        }

        properties["password"] = if (isUsingAWS) AWSSecretHelper().getSecret(passwordSecretName!!) else passwordDummy

        return properties
    }

    @Bean
    fun metadataStoreConnection(): Connection {
        logger.info("Established connection with Metadata Store", "url" to databaseUrl())
        return DriverManager.getConnection(databaseUrl(), databaseProperties())
    }
}
