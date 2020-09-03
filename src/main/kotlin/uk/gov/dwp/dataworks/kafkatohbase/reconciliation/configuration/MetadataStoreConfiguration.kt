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
    var endpoint: String? = "NOT_SET",
    var port: String? = "NOT_SET",
    var user: String? = "NOT_SET",
    var passwordSecretName: String? = "NOT_SET",
    var dummyPassword: String? = "NOT_SET",
    var table: String? = "NOT_SET",
    var databaseName: String? = "NOT_SET",
    var caCertPath: String? = "NOT_SET",
    var queryLimit: String? = "NOT_SET",
    var useAwsSecrets: String? = "NOT_SET"
) {

    companion object {
        val logger = DataworksLogger.getLogger(MetadataStoreConfiguration::class.toString())
    }
    private val isUsingAWS = useAwsSecrets!!.toLowerCase() == "true"

    fun databaseUrl() = "jdbc:mysql://$endpoint:$port/$databaseName"

    fun databaseProperties(): Properties {

        return Properties().apply {
            put("user", user)
            put("useAwsSecrets", useAwsSecrets)

            if (isUsingAWS) {
                put("ssl_ca_path", caCertPath)
                put("ssl_ca", readFile(getProperty("ssl_ca_path")))
                put("ssl_verify_cert", true)
            }
        }
    }

    @Bean
    fun metadataStoreConnection(): Connection {
        val metaStorePassword = if (isUsingAWS) {
            AWSSecretHelper().getSecret(passwordSecretName!!)!!
        } else {
            logger.info("Using dummy password")
            dummyPassword!!
        }
        logger.info("Metastore properties loaded", "metastore_properties" to databaseProperties().toString())
        val metastoreProperties = databaseProperties().apply {
            put("password", metaStorePassword)
        }

        logger.info("Establishing connection with Metadata Store", "url" to databaseUrl())
        val connection = DriverManager.getConnection(databaseUrl(), metastoreProperties)
        logger.info("Established connection with Metadata Store", "url" to databaseUrl())
        return connection
    }
}
