package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.secrets.AWSSecretHelper
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.readFile
import java.sql.Connection
import java.sql.DriverManager
import java.util.*

@Component
@ConfigurationProperties(prefix = "metadatastore")
data class MetadataStoreConfiguration(
    var endpoint: String? = null,
    var port: String? = null,
    var user: String? = null,
    var password: String? = null,
    var table: String? = null,
    var useAwsSecrets: String? = "false",
    var passwordSecretName: String? = "metastore_password",
    var databaseName: String? = "database",
    var caCertPath: String? = "/certs/AmazonRootCA1.pem",
    var queryLimit: String? = "14"
) {

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

        properties["password"] = if (isUsingAWS) AWSSecretHelper().getSecret(passwordSecretName!!) else password

        return properties
    }

    @Bean
    fun metadataStoreConnection(): Connection = DriverManager.getConnection(databaseUrl(), databaseProperties())
}
