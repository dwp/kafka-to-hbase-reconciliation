package uk.gov.dwp.dataworks.kafkatohbase.reconciliation.suppliers.impl

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.test.util.ReflectionTestUtils
import java.sql.Connection
import java.util.*

internal class ConnectionSupplierImplTest {
    @Test
    fun connectionTest() {
        val connection = mock<Connection> {
            on { isClosed } doReturn false
            on { isValid(0) } doReturn true
        }
        val connectionSupplier = ConnectionSupplierImpl("", Properties())
        ReflectionTestUtils.setField(connectionSupplier, "_connection", connection)

        val actual = connectionSupplier.connection()

        verify(connection, times(1)).isValid(0)
        verify(connection, times(1)).isClosed
        verifyNoMoreInteractions(connection)
        assertEquals(connection, actual)
    }
}
