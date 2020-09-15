package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.suppliers

import java.sql.Connection

interface ConnectionSupplier {
    fun connection(): Connection
}
