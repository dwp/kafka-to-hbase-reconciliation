package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.suppliers.impl

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.suppliers.ConnectionSupplier
import java.sql.Connection
import java.sql.DriverManager
import java.util.*

@Component
class ConnectionSupplierImpl(private val databaseUrl: String,
                             private val databaseProperties: Properties): ConnectionSupplier {

    override fun connection(): Connection {
        if (_connection == null || _connection!!.isClosed || !_connection!!.isValid(0)) {
            logger.info("Establishing database connection", "url", databaseUrl)
            _connection = DriverManager.getConnection(databaseUrl, databaseProperties)
        }
        return _connection!!
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ConnectionSupplierImpl::class.java)
    }

    private var _connection: Connection? = null
}
