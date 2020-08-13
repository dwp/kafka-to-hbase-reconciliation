package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.utils

import java.io.File

fun readFile(fileName: String): String
        = File(fileName).readText(Charsets.UTF_8)