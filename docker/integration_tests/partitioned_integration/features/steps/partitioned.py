from behave import given, then, step
import mysql.connector
import time
import logging
import


@given("the {table_name} table has been created and populated, and the reconciler service has been ran")
def step_impl(context, table_name):
    with (mysql.connector.connect(host="metadatastore", user="root", password="password",
                                  database="metadatastore")) as connection:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        assert count > 0
        cursor.close()


@then("the {table_name} table will have {row_count} records")
def step_impl(context, table_name, row_count):
    with (mysql.connector.connect(host="metadatastore", user="root", password="password",
                                  database="metadatastore")) as connection:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        assert count == int(row_count)
        cursor.close()

@then("partition {partition} in the {table_name} table will have {record_count} records")
def step_impl(context, partition, table_name, record_count):
    with (mysql.connector.connect(host="metadatastore", user="root", password="password",
                                  database="metadatastore")) as connection:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} PARTITION ({partition})")
        count = cursor.fetchone()[0]
        assert count == int(record_count)
        cursor.close()


@then("across the {table_name} table there will be {record_count} reconciled records within {timeout} seconds")
def step_impl(context, table_name, record_count, timeout):
        timeout_time = time.time() + int(timeout)
        reconciled = False
        logging.info(f"Timeout time {timeout_time}")
        while time.time() < timeout_time and reconciled is False:
            with (mysql.connector.connect(host="metadatastore", user="root", password="password",
                                          database="metadatastore")) as connection:
                cursor = connection.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE reconciled_result = true")
                count = cursor.fetchone()[0]
                cursor.close()
                if count == int(record_count):
                    assert count == int(record_count)
                    reconciled = True
                    break
                logging.info(f"{count} records reconciled")
            time.sleep(5)

        if reconciled is False:
            raise AssertionError(
                f"Number of records reconciled did not meet expectations of {record_count} within {timeout} seconds."
            )
        else:
            assert reconciled == True