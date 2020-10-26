#!/bin/sh

# Use the metadatastore argument supplied via cmd line or fall back to env var
METADATASTORE_TABLE="${1:-$METADATASTORE_TABLE}"

echo "Running jar using entrypoint METADATASTORE_TABLE=${METADATASTORE_TABLE}"

java -Dmetadatastore.table="${METADATASTORE_TABLE}" -jar ./reconciliation.jar
