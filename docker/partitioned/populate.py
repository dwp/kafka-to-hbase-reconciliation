import mysql.connector
import happybase
from datetime import datetime
import json

topic_count = 10
record_count = 1000

def mysql_connection():
    return mysql.connector.connect(host="metadatastore", user="root", password="password", database="metadatastore")


def hbase_connection():
    return happybase.Connection('hbase')


def populate_hbase():
    connection = hbase_connection()

    for index in range(1, topic_count):
        for record_index in range(1, record_count, 2):
            table_name = f"database:collection{index}"
            table = connection.table(table_name)
            row = json.dumps({ "message": { "_id": str(index)+"/"+str(record_index) } })
            data = hbase_record(
                "database",
                f"collection{index}",
                "{ \"exampleId\": \"aaaa1111-abcd-4567-1234-1234567890ab\"}",
                datetime.now().isoformat()
            )
            table.put(row, data)


def populate_mysql():
    connection = mysql_connection()

    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS equalities")
    cursor.execute("CREATE TABLE equalities LIKE ucfs")

    data = [(f"hbase_id_{index}", index * 100, "db.database.collection", index % 10, index, index % 2 == 0)
            for index in range(0, 1_000)]

    statement = ("INSERT INTO equalities "
                 "(hbase_id, hbase_timestamp, topic_name, kafka_partition, kafka_offset, reconciled_result) "
                 "VALUES (%s, %s, %s, %s, %s, %s)")

    cursor.executemany(statement, data)
    cursor.close()
    connection.commit()


def hbase_record(collection, db_name, id, timestamp):
    return bytes(json.dumps({
        "traceId": "00001111-abcd-4567-1234-1234567890ab",
        "unitOfWorkId": "00002222-abcd-4567-1234-1234567890ab",
        "@type": "V4",
        "version": "core-X.release_XXX.XX",
        "timestamp": "2018-12-14T15:01:02.000+0000",
        "message": {
            "@type": "MONGO_UPDATE",
            "collection": collection,
            "db": db_name,
            "_id": id,
            "_lastModifiedDateTime": timestamp,
            "encryption": {
                "encryptionKeyId": "cloudhsm:1,2",
                "encryptedEncryptionKey": "bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab",
                "initialisationVector": "kjGyvY67jhJHVdo2",
                "keyEncryptionKeyId": "cloudhsm:1,2"
            },
            "dbObject": "bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A",
            "timestamp_created_from": "_lastModifiedDateTime"
        }
    }).encode("utf-8"))


def main():
    populate_mysql()
    populate_hbase()


if __name__ == "__main__":
    main()
