package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.context.properties.bind.DefaultValue
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.secrets.AWSSecretHelper
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.secrets.DummySecretHelper
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.secrets.SecretHelperInterface
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.readFile
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
    var password: String? = null,
    var table: String? = null,
    val useAWSSecrets: String? = "false",
    @DefaultValue("metastore_password") var passwordSecretName: String,
    @DefaultValue("database") var databaseName: String,
    @DefaultValue("/certs/AmazonRootCA1.pem") var caCertPath: String,
    @DefaultValue("14") var queryLimit: String
) {

    private val isUsingAWS = useAWSSecrets == "true"
    private val secretHelper: SecretHelperInterface = if (isUsingAWS) AWSSecretHelper() else DummySecretHelper()

    fun databaseUrl(): String {
        val hostname = endpoint
        val port = port
        return "jdbc:mysql://$hostname:$port/"
    }

    fun databaseProperties(): Properties {

        val properties = Properties().apply {
            put("user", user)
            put("rds.password.secret.name", passwordSecretName)
            put("database", databaseName)
            put("rds.endpoint", endpoint)
            put("rds.port", port)
            put("use.aws.secrets", "false")

            if (isUsingAWS) {
                put("ssl_ca_path", caCertPath)
                put("ssl_ca", readFile(getProperty("ssl_ca_path")))
                put("ssl_verify_cert", true)
            }
        }

        properties["password"] = secretHelper.getSecret(passwordSecretName)

        return properties
    }

    @Bean
    fun metadataStoreConnection(): Connection = DriverManager.getConnection(databaseUrl(), databaseProperties())
}
