package uk.gov.dwp.dataworks.kafkatohbase.reconciliation

import org.apache.hadoop.hbase.client.Connection
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ReconciliationApplication(private val hbaseConnection: Connection, private val metadatastoreConnection: java.sql.Connection): CommandLineRunner {
	override fun run(vararg args: String?) {
		println("$hbaseConnection")
		println("$metadatastoreConnection")
	}
}

fun main(args: Array<String>) {
	runApplication<ReconciliationApplication>(*args)
}
