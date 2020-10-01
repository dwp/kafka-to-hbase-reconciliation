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
        DriverManagerConnectionFactory(databaseUrl, databaseProperties).let { factory ->
            PoolableConnectionFactory(factory, null).let { poolable ->
                GenericObjectPool(poolable).run {
                    poolable.pool = this
                    PoolingDataSource(this)
                }
            }
        }
    }
}
