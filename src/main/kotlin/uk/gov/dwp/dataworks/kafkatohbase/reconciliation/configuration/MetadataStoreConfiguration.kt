package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.secrets.SecretHelperInterface
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.readFile
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.util.*

@Configuration
@ConfigurationProperties(prefix = "metadatastore")
data class MetadataStoreConfiguration(
    var endpoint: String? = "NOT_SET",
    var port: String? = "NOT_SET",
    var user: String? = "NOT_SET",
    var passwordSecretName: String? = "NOT_SET",
    var dummyPassword: String? = "NOT_SET",
    var table: String = "NOT_SET",
    var databaseName: String? = "NOT_SET",
    var caCertPath: String? = "NOT_SET",
    var queryLimit: String? = "NOT_SET",
    var useAwsSecrets: String? = "NOT_SET",
) {

    @Bean
    fun databaseUrl() = "jdbc:mysql://$endpoint:$port/$databaseName"

    @Bean
    fun databaseProperties(): Properties {

        val metaStorePassword = if (isUsingAWS) {
            secretHelper.getSecret(passwordSecretName!!)!!
        } else {
            logger.info("Using dummy password")
            dummyPassword!!
        }

        val properties = Properties().apply {
            put("user", user)
            put("useAwsSecrets", useAwsSecrets)

            if (isUsingAWS) {
                put("ssl_ca_path", caCertPath)
                put("ssl_ca", readFile(getProperty("ssl_ca_path")))
                put("ssl_verify_cert", true)
            }
            put("password", metaStorePassword)
        }

        logger.info("Metadata Store Configuration loaded", "properties" to properties.toString(), "table" to table, "query_limit" to queryLimit.toString())

        return properties
    }

    @Bean
    @Qualifier("table")
    fun table() = table

    @Bean
    @Qualifier("queryLimit")
    fun queryLimit() = queryLimit

    @Autowired
    private lateinit var secretHelper: SecretHelperInterface

    companion object {
        val logger = DataworksLogger.getLogger(MetadataStoreConfiguration::class.toString())
    }

    private val isUsingAWS by lazy { this.useAwsSecrets!!.toLowerCase() == "true" }
}
