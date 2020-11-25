from behave import given, then, step
import mysql.connector


@given("the {table_name} table has been created and populated, and the reconciler service has been ran")
def step_impl(context, table_name):
    with () as connection:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        assert count > 0


@then("the {table_name} table will have {row_count} records")
def step_impl(context, table_name, row_count):
    with (mysql.connector.connect(host="metadatastore", user="root", password="password",
                                  database="metadatastore")) as connection:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        assert count == int(row_count)


@step("the {table_name} table will have {row_count} reconciled records")
def step_impl(context, table_name, row_count):
    with (mysql.connector.connect(host="metadatastore", user="root", password="password",
                                  database="metadatastore")) as connection:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} where reconciled_result = true")
        count = cursor.fetchone()[0]
        assert count == row_count
