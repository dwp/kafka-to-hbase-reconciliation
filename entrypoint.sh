#!/bin/sh

METADATASTORE_TABLE_VALUE="${1:-$METADATASTORE_TABLE}"

echo "Running jar using entrypoint METADATASTORE_TABLE=${METADATASTORE_TABLE_VALUE}"

java -Dmetadatastore.table="${METADATASTORE_TABLE_VALUE}" -jar ./reconciliation.jar
