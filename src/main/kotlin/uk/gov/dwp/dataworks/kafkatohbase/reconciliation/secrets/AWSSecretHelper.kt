package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.secrets

import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.SecretsManagerConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services.ReconciliationService
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Component
class AWSSecretHelper : SecretHelperInterface {

    companion object {
        val logger = DataworksLogger.getLogger(ReconciliationService::class.toString())
    }

    val configuration: SecretsManagerConfiguration = SecretsManagerConfiguration()

    override fun getSecret(secretName: String): String? {

        val client = configuration.secretsManagerClient()

        logger.info("Getting value from aws secret manager", "secret_name" to secretName)

        try {
            val getSecretValueRequest = GetSecretValueRequest().withSecretId(secretName)

            val getSecretValueResult = client.getSecretValue(getSecretValueRequest)

            logger.debug("Successfully got value from aws secret manager", "secret_name" to secretName)

            val secretString = getSecretValueResult.secretString

            @Suppress("UNCHECKED_CAST")
            val map = ObjectMapper().readValue(secretString, Map::class.java) as Map<String, String>

            return map["password"]
        } catch (e: Exception) {
            logger.error("Failed to get aws secret manager result", e, "secret_name" to secretName)
            throw e
        }
    }
}
