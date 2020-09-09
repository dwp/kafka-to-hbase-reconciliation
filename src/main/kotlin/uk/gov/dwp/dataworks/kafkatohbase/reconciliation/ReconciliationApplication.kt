package uk.gov.dwp.dataworks.kafkatohbase.reconciliation

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler


@ConfigurationPropertiesScan
@SpringBootApplication
@EnableScheduling
class ReconciliationApplication

fun main(args: Array<String>) {
    //exitProcess(SpringApplication.exit(runApplication<ReconciliationApplication>(*args)))
    runApplication<ReconciliationApplication>(*args)
}

// This makes sure any scheduled tasks complete before shutting down
@Bean
fun setSchedulerToWait(threadPoolTaskScheduler: ThreadPoolTaskScheduler): ThreadPoolTaskScheduler? {
    threadPoolTaskScheduler.setWaitForTasksToCompleteOnShutdown(true)
    return threadPoolTaskScheduler
}
