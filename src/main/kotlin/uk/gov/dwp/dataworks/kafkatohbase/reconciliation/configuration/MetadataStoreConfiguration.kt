package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.exceptions.MetadataStorePartitionsNotSetException
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.secrets.SecretHelperInterface
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.readFile
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.toPartitionIdCSV
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils.validatePartitions
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
    var useAwsSecrets: Boolean = true,
    var numberOfParallelUpdates: String? = "NOT_SET",
    var batchSize: String? = "NOT_SET",
    var deleteLimit: Int = 100_000,
    var startPartition: String? = "NOT_SET",
    var endPartition: String? = "NOT_SET") {

    @Bean
    fun databaseUrl() = "jdbc:mysql://$endpoint:$port/$databaseName"

    @Bean
    fun databaseProperties(): Properties =
        Properties().apply {
            put("user", user)
            if (useAwsSecrets) {
                put("ssl_ca_path", caCertPath)
                put("ssl_ca", readFile(getProperty("ssl_ca_path")))
                put("ssl_verify_cert", true)
            }
            put("password", password(useAwsSecrets))
        }

    private fun password(useAwsSecrets: Boolean) =
        if (useAwsSecrets) {
            secretHelper.getSecret(passwordSecretName!!)!!
        } else {
            dummyPassword!!
        }

    @Bean
    @Qualifier("table")
    fun table() = table

    @Bean
    @Qualifier("numberOfParallelUpdates")
    fun numberOfParallelUpdates() = numberOfParallelUpdates!!.toInt()

    @Bean
    @Qualifier("batchSize")
    fun batchSize() = batchSize!!.toInt()

    @Bean
    fun deleteLimit() = deleteLimit

    @Bean
    @Qualifier("partitions")
    fun partitions(): String {
        validatePartitions(startPartition, endPartition)
        logger.info("Partitions validated for metadata store", "start_partition" to startPartition!!, "end_partition" to endPartition!!)
        return toPartitionIdCSV(startPartition!!, endPartition!!)
    }

    @Autowired
    private lateinit var secretHelper: SecretHelperInterface

    companion object {
        val logger = DataworksLogger.getLogger(MetadataStoreConfiguration::class.java.toString())
    }
}
