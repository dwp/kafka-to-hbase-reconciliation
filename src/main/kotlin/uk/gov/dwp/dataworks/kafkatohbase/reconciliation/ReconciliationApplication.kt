package uk.gov.dwp.dataworks.kafkatohbase.reconciliation

import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.ComponentScan
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.HbaseConfig
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.MetadataStoreConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.SecretsManagerConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService

//@EnableConfigurationProperties(MetadataStoreConfiguration::class,HbaseConfig::class,SecretsManagerConfiguration::class)
@ConfigurationPropertiesScan
@SpringBootApplication
class ReconciliationApplication(private val reconciliationService: ReconciliationService) : CommandLineRunner {
    override fun run(vararg args: String?) {
        reconciliationService.startReconciliation()
    }
}

fun main(args: Array<String>) {
    runApplication<ReconciliationApplication>(*args)
}
