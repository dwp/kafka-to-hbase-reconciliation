package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.secrets

import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.SecretsManagerConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ScheduledReconciliationService
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Component
@Profile("DUMMY_SECRETS")
class DummySecretHelper: SecretHelperInterface {

    companion object {
        val logger = DataworksLogger.getLogger(ScheduledReconciliationService::class.toString())
    }

    val configuration: SecretsManagerConfiguration = SecretsManagerConfiguration()

    override fun getSecret(secretName: String): String? {

        logger.info("Getting value from dummy secret manager", "secret_name" to secretName)

        try {
            return configuration.dummySecret ?: "NOT_SET"
        } catch (e: Exception) {
            logger.error("Failed to get dummy secret manager result", e, "secret_name" to secretName)
            throw e
        }
    }
}
