package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration

import com.amazonaws.services.secretsmanager.AWSSecretsManager
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration
@Profile("!DUMMY_SECRETS")
@ConfigurationProperties(prefix = "secrets")
data class SecretsManagerConfiguration(
    var region: String? = "NOT_SET",
    var dummySecret: String? = "NOT_SET"
) {

    @Bean
    fun secretsManagerClient(): AWSSecretsManager = AWSSecretsManagerClientBuilder.standard().withRegion(region).build()
}
