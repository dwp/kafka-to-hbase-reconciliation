import argparse
import binascii
import json
import sys

import requests

import shared_functions

topic_count = 10
record_count = 1000


def populate_hbase():
    connection = shared_functions.hbase_connection()
    connection.open()

    args = command_line_args()
    content = requests.get(args.data_key_service).json()
    encryption_key = content['plaintextDataKey']
    encrypted_key = content['ciphertextDataKey']
    master_key_id = content['dataKeyEncryptionKeyId']

    shared_functions.cleanup_hbase(connection)

    for index in range(1, topic_count):

        print(f'Creating for topic {index}')
        table_name = f"database:collection{index}"
        table = connection.table(table_name)
        batch = table.batch(timestamp=1000)

        for record_index in range(1, record_count, 2):
            wrapper = shared_functions.kafka_message(record_index)
            record = shared_functions.decrypted_db_object(record_index)
            record_string = json.dumps(record)
            [iv, encrypted_record] = shared_functions.encrypt(encryption_key, record_string)
            wrapper['message']['encryption']['initialisationVector'] = iv.decode('ascii')
            wrapper['message']['encryption']['keyEncryptionKeyId'] = master_key_id
            wrapper['message']['encryption']['encryptedEncryptionKey'] = encrypted_key
            wrapper['message']['dbObject'] = encrypted_record.decode('ascii')
            message_id = json.dumps(wrapper['message']['_id'])
            checksum = binascii.crc32(message_id.encode("ASCII"), 0).to_bytes(4, sys.byteorder)
            hbase_id = checksum + message_id.encode("utf-8")
            obj = {'cf:record': json.dumps(wrapper)}
            batch.put(hbase_id, obj)
        print("Sending batch.")
        batch.send()

    connection.close()


def populate_mysql():
    connection = shared_functions.mysql_connection()

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


def command_line_args():
    parser = argparse.ArgumentParser(description='Pre-populate hbase for profiling.')
    parser.add_argument('-k', '--data-key-service', default='http://dks-standalone-http:8080/datakey',
                        help='Use the specified data key service.')
    parser.add_argument('-z', '--zookeeper-quorum', default='hbase',
                        help='The zookeeper quorum host.')
    parser.add_argument('-r', '--records', default='10000',
                        help='The number of records to create.')
    return parser.parse_args()


def main():
    populate_mysql()
    populate_hbase()


if __name__ == "__main__":
    main()
