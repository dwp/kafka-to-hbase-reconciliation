#!/bin/sh

METADATASTORE_TABLE_VALUE="${1:-$METADATASTORE_TABLE}"

export METADATASTORE_TRUSTSTORE=./truststore.jks
export METADATASTORE_TRUSTSTORE_PASSWORD="$(uuidgen)"

keytool -noprompt -import \
  -file "$METADATA_TRUSTSTORE_CERTIFICATE" \
  -keystore ./truststore.jks \
  -storepass "$METADATASTORE_TRUSTSTORE_PASSWORD"

java -Dmetadatastore.table="${METADATASTORE_TABLE_VALUE}" -jar ./reconciliation.jar
