#!/bin/sh

METADATASTORE_TABLE="${1:-NOT_SET_IN_ENTRYPOINT}"

echo "Running jar using entrypoint METADATASTORE_TABLE=${METADATASTORE_TABLE}"

java -Dmetadatastore.table="${METADATASTORE_TABLE}" -jar ./reconciliation.jar
