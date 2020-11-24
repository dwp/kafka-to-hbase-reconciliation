import base64
import binascii
import json
from datetime import datetime
import argparse
import happybase
import mysql.connector
from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter
import requests

topic_count = 10
record_count = 1000


def mysql_connection():
    return mysql.connector.connect(host="metadatastore", user="root", password="password", database="metadatastore")


def hbase_connection():
    return happybase.Connection('hbase')


def populate_hbase():
    connection = hbase_connection()
    connection.open()

    args = command_line_args()
    content = requests.get(args.data_key_service).json()
    encryption_key = content['plaintextDataKey']
    encrypted_key = content['ciphertextDataKey']
    master_key_id = content['dataKeyEncryptionKeyId']

    for index in range(1, topic_count):

        print(f'Creating for topic {index}')
        table_name = f"database:collection{index}"
        table = connection.table(table_name)
        batch = table.batch(timestamp=1000)

        for record_index in range(1, record_count, 2):
            wrapper = kafka_message(record_index)
            record = decrypted_db_object(record_index)
            record_string = json.dumps(record)
            [iv, encrypted_record] = encrypt(encryption_key, record_string)
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

    # for index in range(1, topic_count):
    #     print(f'Creating for topic {index}')
    #     for record_index in range(1, record_count, 2):
    #         print(f'Creating for record {record_index}')
    #         table_name = f"database:collection{index}"
    #         table = connection.table(table_name)
    #         row = json.dumps({"message": {"_id": str(index) + "/" + str(record_index)}})
    #         data = hbase_record(
    #             "database".encode('utf-8'),
    #             f"collection{index}".encode('utf-8'),
    #             "{ \"exampleId\": \"aaaa1111-abcd-4567-1234-1234567890ab\"}".encode('utf-8'),
    #             datetime.now().isoformat().encode('utf-8')
    #         )
    #         table.put(row, data)


def encrypt(key, plaintext):
    initialisation_vector = Random.new().read(AES.block_size)
    iv_int = int(binascii.hexlify(initialisation_vector), 16)
    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(base64.b64decode(key), AES.MODE_CTR, counter=counter)
    ciphertext = aes.encrypt(plaintext.encode("utf8"))
    return (base64.b64encode(initialisation_vector),
            base64.b64encode(ciphertext))


def kafka_message(i: int):
    return {
        "traceId": f"{i:05d}",
        "unitOfWorkId": f"{i:05d}",
        "@type": "V4",
        "message": {
            "db": "database",
            "collection": "collection",
            "_id": {
                "record_id": f"{i:05d}"
            },
            "_timeBasedHash": "hash",
            "@type": "MONGO_INSERT",
            "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000",
            "encryption": {
                "encryptionKeyId": "",
                "encryptedEncryptionKey": "",
                "initialisationVector": "",
                "keyEncryptionKeyId": ""
            },
            "dbObject": ""
        },
        "version": "core-4.master.9790",
        "timestamp": "2019-07-04T07:27:35.104+0000"
    }


def decrypted_db_object(i: int):
    return {
        "_id": {"record_id": f"{i:05d}"} if i % 2 == 0 else f"{i:05d}",
        "createdDateTime": "2015-03-20T12:23:25.183Z",
        "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000"
    }

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
    return {
        b"cf:traceId": b"00001111-abcd-4567-1234-1234567890ab",
        b"cf:unitOfWorkId": b"00002222-abcd-4567-1234-1234567890ab",
        b"cf:@type": b"V4",
        b"cf:version": b"core-X.release_XXX.XX",
        b"cf:timestamp": b"2018-12-14T15:01:02.000+0000",
        b"cf:message": b"123"
    }
# {
#     b"@type": b"MONGO_UPDATE",
#     b"collection": collection,
#     b"db": db_name,
#     b"_id": id,
#     b"_lastModifiedDateTime": timestamp,
#     b"encryption": {
#         b"encryptionKeyId": b"cloudhsm:1,2",
#         b"encryptedEncryptionKey": b"bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab",
#         b"initialisationVector": b"kjGyvY67jhJHVdo2",
#         b"keyEncryptionKeyId": b"cloudhsm:1,2"
#     },
#     b"dbObject": b"bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A",
#     b"timestamp_created_from": b"_lastModifiedDateTime"
# }


def main():
    populate_mysql()
    populate_hbase()


def command_line_args():
    parser = argparse.ArgumentParser(description='Pre-populate hbase for profiling.')
    parser.add_argument('-k', '--data-key-service', default='http://dks-standalone-http:8443/datakey',
                        help='Use the specified data key service.')
    parser.add_argument('-z', '--zookeeper-quorum', default='hbase',
                        help='The zookeeper quorum host.')
    parser.add_argument('-r', '--records', default='10000',
                        help='The number of records to create.')
    return parser.parse_args()


if __name__ == "__main__":
    main()
