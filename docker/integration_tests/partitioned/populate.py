import argparse
import binascii
import json
import re
import sys

import requests

import shared_functions

topic_count = 10
record_count = 1000


def populate_mysql():
    connection = shared_functions.mysql_connection()

    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS equalities")
    cursor.execute("""CREATE TABLE IF NOT EXISTS `equalities` (
                        `id` INT NOT NULL AUTO_INCREMENT,
                        `hbase_id` VARCHAR(2048) NULL,
                        `hbase_timestamp` BIGINT NULL,
                        `write_timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
                        `correlation_id` VARCHAR(160) NULL,
                        `topic_name` VARCHAR(160) NULL,
                        `kafka_partition` INT NULL,
                        `kafka_offset` INT NULL,
                        `reconciled_result` TINYINT(1) NOT NULL DEFAULT 0,
                        `reconciled_timestamp` DATETIME NULL,
                        `last_checked_timestamp` DATETIME NULL,
                        PRIMARY KEY (`id`),
                        INDEX (hbase_id,hbase_timestamp),
                        INDEX (write_timestamp),
                        INDEX (reconciled_result),
                        INDEX (last_checked_timestamp)
                    )
                        PARTITION BY HASH(id)
                        PARTITIONS 4;""")

    data = []
    for topic_index in range(int(topic_count)):

        table_name = f"db.database.collection{topic_index}"

        for record_index in range(int(record_count)):
            hbase_key = f"{topic_index}/{record_index}"
            ob = {"_id": hbase_key}
            checksum = binascii.crc32(json.dumps(ob).encode("ASCII"), 0).to_bytes(4, 'big').hex().upper()
            escaped = re.sub("(..)", r"\\x\1", checksum)
            timestamp = 1544799662000
            key = f"{escaped}{ob}"
            data.append(
                [key, timestamp, table_name, 0])
            print(f"escaped: {key}")

    statement = ("INSERT INTO equalities "
                 "(hbase_id, hbase_timestamp, topic_name, reconciled_result) "
                 "VALUES (%s, %s, %s, %s)")

    cursor.executemany(statement, data)
    cursor.close()
    connection.commit()


def populate_hbase():
    connection = shared_functions.hbase_connection()
    connection.open()

    args = command_line_args()
    content = requests.get(args.data_key_service).json()
    encryption_key = content['plaintextDataKey']
    encrypted_key = content['ciphertextDataKey']
    master_key_id = content['dataKeyEncryptionKeyId']

    print("Creating batch.")
    for topic_index in range(int(topic_count)):

        table_name = f"database:collection{topic_index}"
        tables = [x.decode('ascii') for x in connection.tables()]

        if table_name not in tables:
            connection.create_table(table_name, {'cf': dict(max_versions=1000000)})
            print(f"Created table '{table_name}'.")

        table = connection.table(table_name)
        batch = table.batch(timestamp=10000)

        print("test")
        for record_index in range(0, int(record_count), 2):
            wrapper = shared_functions.kafka_message(topic_index)
            record = shared_functions.decrypted_db_object(topic_index)
            record_string = json.dumps(record)
            [iv, encrypted_record] = shared_functions.encrypt(encryption_key, record_string)
            wrapper['message']['encryption']['initialisationVector'] = iv.decode('ascii')
            wrapper['message']['encryption']['keyEncryptionKeyId'] = master_key_id
            wrapper['message']['encryption']['encryptedEncryptionKey'] = encrypted_key
            wrapper['message']['dbObject'] = encrypted_record.decode('ascii')
            message_id = json.dumps(wrapper['message']['_id'])
            checksum = binascii.crc32(message_id.encode("ASCII"), 0).to_bytes(4, sys.byteorder)
            key = json.dumps({"message": {"_id": f"{topic_index}/{record_index}"}}).encode("utf-8")
            hbase_id = checksum + key
            obj = {'cf:record': json.dumps(wrapper)}

            print(f"hbase_id: {hbase_id}")
            batch.put(hbase_id, obj)

        print("Sending batch.")
        batch.send()

    connection.close()
    print("Done.")


def command_line_args():
    parser = argparse.ArgumentParser(description='Pre-populate hbase for profiling.')
    parser.add_argument('-k', '--data-key-service', default='http://dks-standalone-http:8080/datakey',
                        help='Use the specified data key service.')
    parser.add_argument('-z', '--zookeeper-quorum', default='hbase',
                        help='The zookeeper quorum host.')
    parser.add_argument('-r', '--records', default='1000',
                        help='The number of records to create.')
    return parser.parse_args()


def main():
    populate_mysql()
    populate_hbase()


if __name__ == "__main__":
    main()
