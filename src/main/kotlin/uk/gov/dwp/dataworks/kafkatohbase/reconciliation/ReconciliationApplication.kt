package uk.gov.dwp.dataworks.kafkatohbase.reconciliation

import org.apache.hadoop.hbase.client.Connection
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService

@SpringBootApplication
class ReconciliationApplication(private val reconciliationService: ReconciliationService) : CommandLineRunner {
    override fun run(vararg args: String?) {
        reconciliationService.reconciliation()
    }


}

fun main(args: Array<String>) {
    runApplication<ReconciliationApplication>(*args)
}
