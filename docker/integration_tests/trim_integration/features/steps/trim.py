from behave import given, then, step
import mysql.connector


@given("the {table_name} table has been created and populated and the trim service has been run")
def step_impl(context, table_name):
    with (mysql.connector.connect(host="metadatastore", user="root", password="password",
                                  database="metadatastore")) as connection:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        assert count > 0


@then("the {table_name} table will have {row_count} rows")
def step_impl(context, table_name, row_count):
    with (mysql.connector.connect(host="metadatastore", user="root", password="password",
                                  database="metadatastore")) as connection:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]

        print(f"count: {count}")
        print(f"table_name: {table_name}")

        assert count == int(row_count)


@step("none of the records on the {table_name} table will be reconciled")
def step_impl(context, table_name):
    with (mysql.connector.connect(host="metadatastore", user="root", password="password",
                                  database="metadatastore")) as connection:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} where reconciled_result = true")
        count = cursor.fetchone()[0]
        assert count == 0
