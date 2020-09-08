package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "reconciler")
data class ReconcilerConfiguration(
    var trimRecordsFixedDelayMillis: String? = "NOT_SET"
)