package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.suppliers.impl

import org.apache.commons.dbcp2.DriverManagerConnectionFactory
import org.apache.commons.dbcp2.PoolableConnectionFactory
import org.apache.commons.dbcp2.PoolingDataSource
import org.apache.commons.pool2.impl.GenericObjectPool
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.suppliers.ConnectionSupplier
import java.sql.Connection
import java.util.*

@Component
@Profile("!SINGLE_CONNECTION")
class PooledConnectionSupplier(private val databaseUrl: String,
                               private val databaseProperties: Properties) : ConnectionSupplier {

    override fun connection(): Connection = dataSource.connection


    private val dataSource by lazy {
        val connectionFactory = DriverManagerConnectionFactory(databaseUrl, databaseProperties)
        val poolableConnectionFactory = PoolableConnectionFactory(connectionFactory, null)
        val connectionPool = GenericObjectPool(poolableConnectionFactory)
        poolableConnectionFactory.pool = connectionPool
        PoolingDataSource(connectionPool)
    }
}
