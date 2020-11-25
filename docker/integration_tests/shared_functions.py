import happybase
import mysql.connector
import base64
from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter
import binascii


def mysql_connection():
    return mysql.connector.connect(host="metadatastore", user="root", password="password", database="metadatastore")


def hbase_connection():
    return happybase.Connection('hbase')


def cleanup_hbase(connection):
    for table in connection.tables():
        connection.disable_table(table)
        connection.delete_table(table)


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
