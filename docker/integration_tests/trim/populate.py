from shared_functions import mysql_connection

if __name__ == "__main__":

    connection = mysql_connection()

    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS trim")
    cursor.execute("CREATE TABLE trim LIKE ucfs")

    data = [
        (
            f"hbase_id_{index}",
            index * 100,
            "db.database.collection",
            index % 10,
            index,
            index % 2 == 0,
        )
        for index in range(1, 1000, 2)
    ]

    statement = (
        "INSERT INTO trim "
        "(hbase_id, hbase_timestamp, topic_name, kafka_partition, kafka_offset, reconciled_result) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )

    cursor.executemany(statement, data)
    cursor.close()
    connection.commit()
