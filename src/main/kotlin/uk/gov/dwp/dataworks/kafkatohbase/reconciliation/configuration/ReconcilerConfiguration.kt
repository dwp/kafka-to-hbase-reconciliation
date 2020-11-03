package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "reconciler")
data class ReconcilerConfiguration(var minimumAgeScale: Int = 10,
                                   var minimumAgeUnit: String = "MINUTE",
                                   var optimizeAfterDelete: Boolean = true,
                                   var autoCommitStatements: Boolean = false,
                                   var lastCheckedUnit: String = "MINUTE",
                                   var lastCheckedScale: Int = 30) {

    @Bean
    fun minimumAgeScale() = minimumAgeScale

    @Bean
    fun minimumAgeUnit() = minimumAgeUnit

    @Bean
    fun autoCommitStatements() = autoCommitStatements

    @Bean
    fun optimizeAfterDelete() = optimizeAfterDelete

    @Bean
    fun lastCheckedUnit() = lastCheckedUnit

    @Bean
    fun lastCheckedScale() = lastCheckedScale
}
