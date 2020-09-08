package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.domain

data class UnreconciledRecord(val topicName: String, val hbaseId: String, val version: Long)


