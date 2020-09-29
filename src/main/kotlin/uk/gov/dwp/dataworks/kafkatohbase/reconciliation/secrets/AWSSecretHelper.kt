package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.secrets

import com.amazonaws.services.secretsmanager.AWSSecretsManager
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Component
@Profile("!DUMMY_SECRETS")
class AWSSecretHelper(private val secretsManager: AWSSecretsManager) : SecretHelperInterface {

    companion object {
        val logger = DataworksLogger.getLogger(AWSSecretHelper::class.java.toString())
    }

    override fun getSecret(secretName: String): String? {

        logger.info("Getting value from aws secret manager", "secret_name" to secretName)

        try {
            val getSecretValueRequest = GetSecretValueRequest().withSecretId(secretName)

            val getSecretValueResult = secretsManager.getSecretValue(getSecretValueRequest)

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
