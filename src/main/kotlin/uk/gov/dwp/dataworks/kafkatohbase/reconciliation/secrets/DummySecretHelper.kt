package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.secrets

import ch.qos.logback.core.util.OptionHelper.getEnv
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Component
class DummySecretHelper: SecretHelperInterface {

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationService::class.toString())
    }

    override fun getSecret(secretName: String): String? {

        logger.info("Getting value from dummy secret manager", "secret_name" to secretName)

        try {
            return getEnv("DUMMY_SECRET_${secretName.toUpperCase()}") ?: "NOT_SET"
        } catch (e: Exception) {
            logger.error("Failed to get dummy secret manager result", e, "secret_name" to secretName)
            throw e
        }
    }
}
