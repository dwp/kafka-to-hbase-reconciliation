package uk.gov.dwp.dataworks.kafkatohbase.reconciliation

import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService

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
