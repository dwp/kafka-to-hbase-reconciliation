#!/bin/sh

<<<<<<< HEAD
METADATASTORE_TABLE_VALUE="${1:-NOT_SET_IN_ENTRYPOINT}"
=======
# Use the metadatastore argument supplied via cmd line or fall back to env var
METADATASTORE_TABLE="${1:-$METADATASTORE_TABLE}"
>>>>>>> 4895e043b48780a184b72cdab94c10457366efb0

echo "Running jar using entrypoint METADATASTORE_TABLE=${METADATASTORE_TABLE_VALUE}"

java -Dmetadatastore.table="${METADATASTORE_TABLE_VALUE}" -jar ./reconciliation.jar
