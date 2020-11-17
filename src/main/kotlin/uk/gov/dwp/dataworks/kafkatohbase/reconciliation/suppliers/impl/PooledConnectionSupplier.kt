package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.suppliers.impl

import org.apache.commons.dbcp2.BasicDataSource
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.configuration.MetadataStoreConfiguration
import uk.gov.dwp.dataworks.kafkatohbase.reconciliation.suppliers.ConnectionSupplier
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.sql.Connection
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.hours

@Component
@Profile("!SINGLE_CONNECTION")
@ExperimentalTime
class PooledConnectionSupplier(private val databaseUrl: String,
                               private val databaseProperties: Properties,
                               private val numberOfParallelUpdates: Int,
                               private val table: String) : ConnectionSupplier {

    override fun connection(): Connection = dataSource.connection

    private val dataSource by lazy {
        logger.info("Datasource configuration",
            "url" to databaseUrl,
            "user" to "${databaseProperties.get("user")}",
            "table" to table,
            "numberOfParallelUpdates" to "$numberOfParallelUpdates")

        BasicDataSource().apply {
            url = databaseUrl
            maxTotal = numberOfParallelUpdates + 1 + 5
            maxConnLifetimeMillis = 24.hours.toLongMilliseconds()
            databaseProperties.forEach { (name, value) ->
                addConnectionProperty(name.toString(), value.toString())
            }
        }
    }


    companion object {
        val logger = DataworksLogger.getLogger(MetadataStoreConfiguration::class.java.toString())
    }
}
