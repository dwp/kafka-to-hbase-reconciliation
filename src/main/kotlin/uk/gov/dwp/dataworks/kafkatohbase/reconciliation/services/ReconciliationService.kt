package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.services

import org.springframework.stereotype.Service

@Service
interface ReconciliationService {
    fun start()
}
