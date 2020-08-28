package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.secrets

interface SecretHelperInterface {
    fun getSecret(secretName: String): String?
}
