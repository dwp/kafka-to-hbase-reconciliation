package uk.gov.dwp.dataworks.kafkatohbase.reconciliation

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ScheduledReconciliationService


@ConfigurationPropertiesScan
@SpringBootApplication
@EnableScheduling
class ReconciliationApplication: CommandLineRunner {
    override fun run(vararg args: String?) {
        if (service !is ScheduledReconciliationService) {
            service.start()
        }
    }

    @Autowired
    private lateinit var service: ReconciliationService

    @Value("\${spring.profiles.active}")
    private lateinit var activeProfiles: String
}

fun main(args: Array<String>) {
    runApplication<ReconciliationApplication>(*args)
}

// This makes sure any scheduled tasks complete before shutting down
@Bean
fun setSchedulerToWait(threadPoolTaskScheduler: ThreadPoolTaskScheduler): ThreadPoolTaskScheduler? {
    threadPoolTaskScheduler.setWaitForTasksToCompleteOnShutdown(true)
    return threadPoolTaskScheduler
}
