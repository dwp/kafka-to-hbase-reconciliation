package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import com.amazonaws.services.secretsmanager.AWSSecretsManager
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "secrets")
@EnableConfigurationProperties
class SecretsManagerConfiguration(
    private var region: String? = null,
    internal val metadataStorePasswordSecret: String? = null
) {

    @Bean
    fun secretsManagerClient(): AWSSecretsManager = AWSSecretsManagerClientBuilder.standard().withRegion(region).build()
}
