import argparse
import binascii
import json
import re
import sys

import requests

import shared_functions

topic_count = 10
record_count = 100

def populate_mysql(database_table_name):
    connection = shared_functions.mysql_connection()

    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {database_table_name}")
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS `{database_table_name}` (
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
    for topic_index in range(1, int(topic_count)+1):

        table_name = f"db.database.{database_table_name}{topic_index}"

        for record_index in range(1, int(record_count)+1):
            hbase_key = f"{topic_index}/{record_index}"
            ob = {"id": hbase_key}
            json_object = json.dumps(ob, separators=(',', ':'))
            checksum = binascii.crc32(json_object.encode("ASCII"), 0).to_bytes(4, "big").hex().upper()
            escaped = re.sub("(..)", r"\\x\1", checksum)

            timestamp = 1544799662000
            key = f'{escaped}{json_object}'
            data.append(
                [key, timestamp, table_name, 0])
            print(f'escaped: {key}')

    statement = (f"INSERT INTO {database_table_name} "
                 "(hbase_id, hbase_timestamp, topic_name, reconciled_result) "
                 "VALUES (%s, %s, %s, %s)")

    cursor.executemany(statement, data)
    cursor.close()
    connection.commit()


def populate_hbase(database_table_name):
    connection = shared_functions.hbase_connection()
    connection.open()

    args = command_line_args()
    content = requests.get(args.data_key_service).json()
    encryption_key = content['plaintextDataKey']
    encrypted_key = content['ciphertextDataKey']
    master_key_id = content['dataKeyEncryptionKeyId']

    print("Creating batch.")
    for topic_index in range(1, int(topic_count)+1):

        table_name = f"database:{database_table_name}{topic_index}"
        tables = [x.decode('ascii') for x in connection.tables()]

        if table_name not in tables:
            connection.create_table(table_name, {'cf': dict(max_versions=1000000)})
            print(f"Created table '{table_name}'.")

        table = connection.table(table_name)
        batch = table.batch(timestamp=1544799662000)

        for record_index in range(2, int(record_count) + 1, 2):
            wrapper = shared_functions.kafka_message(topic_index)
            record = shared_functions.decrypted_db_object(topic_index)
            record_string = json.dumps(record)
            [iv, encrypted_record] = shared_functions.encrypt(encryption_key, record_string)
            wrapper['message']['encryption']['initialisationVector'] = iv.decode('ascii')
            wrapper['message']['encryption']['keyEncryptionKeyId'] = master_key_id
            wrapper['message']['encryption']['encryptedEncryptionKey'] = encrypted_key
            wrapper['message']['dbObject'] = encrypted_record.decode('ascii')

            hbase_key = f"{topic_index}/{record_index}"
            record_id = {"id": hbase_key}
            json_object = json.dumps(record_id, separators=(',', ':'))
            checksum = binascii.crc32(json_object.encode("ASCII"), 0).to_bytes(4, "big")
            hbase_id = checksum + json_object.encode()
            record_object = {'cf:record': json.dumps(wrapper)}
            batch.put(hbase_id, record_object)

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
    populate_mysql("equalities")
    populate_mysql("ucfs")
    populate_hbase("equalities")
    populate_hbase("ucfs")


if __name__ == "__main__":
    main()
