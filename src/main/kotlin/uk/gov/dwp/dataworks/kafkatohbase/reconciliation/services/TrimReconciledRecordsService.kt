package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services

interface TrimReconciledRecordsService {
    fun trimReconciledRecordsOnTimer()
    fun trimReconciledRecords()
}