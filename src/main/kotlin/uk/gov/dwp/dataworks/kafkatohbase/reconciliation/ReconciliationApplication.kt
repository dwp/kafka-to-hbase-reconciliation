package uk.gov.dwp.dataworks.kafkatohbase.reconciliation

//import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling
//import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import kotlin.system.exitProcess

@ConfigurationPropertiesScan
@SpringBootApplication
@EnableScheduling
class ReconciliationApplication
//    (private val reconciliationService: ReconciliationService) : CommandLineRunner {
//    override fun run(vararg args: String?) {
//        reconciliationService.startReconciliation()
//    }
//}

fun main(args: Array<String>) {
    //exitProcess(SpringApplication.exit(runApplication<ReconciliationApplication>(*args)))
    runApplication<ReconciliationApplication>(*args)
}
