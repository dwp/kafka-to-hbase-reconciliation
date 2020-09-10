package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "reconciler")
data class ReconcilerConfiguration(
    var trimRecordsFixedDelayMillis: String? = "NOT_SET",
    var trimReconciledScale: String? = "NOT_SET",
    var trimReconciledUnit: String? = "NOT_SET"
) {

    @Bean
    @Qualifier("trimReconciledScale")
    fun trimReconciledScale() = trimReconciledScale!!

    @Bean
    @Qualifier("trimReconciledUnit")
    fun trimReconciledUnit() = trimReconciledUnit!!
}
