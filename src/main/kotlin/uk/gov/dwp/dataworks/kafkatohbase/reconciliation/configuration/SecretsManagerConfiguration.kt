package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import com.amazonaws.services.secretsmanager.AWSSecretsManager
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Configuration
@Profile("usingAws")
@ConfigurationProperties(prefix = "secrets")
data class SecretsManagerConfiguration(
    var region: String? = null,
    var metadataStorePasswordSecret: String? = null
) {

    @Bean
    fun secretsManagerClient(): AWSSecretsManager = AWSSecretsManagerClientBuilder.standard().withRegion(region).build()
}
