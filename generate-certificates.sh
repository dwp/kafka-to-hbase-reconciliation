#!/bin/bash

main() {
    make_keystore dks-keystore.jks dks-standalone-https
    extract_public_certificate dks-keystore.jks dks-standalone-https.crt
    make_truststore dks-truststore.jks dks-standalone-https.crt

    make_keystore keystore.jks kafka-to-hbase-reconciliation
    extract_public_certificate keystore.jks kafka-to-hbase-reconciliation.crt
    make_truststore truststore.jks kafka-to-hbase-reconciliation.crt

    import_into_truststore dks-truststore.jks kafka-to-hbase-reconciliation.crt kafka-to-hbase-reconciliation
    import_into_truststore truststore.jks dks-standalone-https.crt dks

    mv -v dks-truststore.jks docker/dks
    mv -v dks-keystore.jks docker/dks
}

make_keystore() {
    local keystore="${1:?Usage: $FUNCNAME keystore common-name}"
    local common_name="${2:?Usage: $FUNCNAME keystore common-name}"

    [[ -f "${keystore}" ]] && rm -v "${keystore}"

    keytool -v \
            -genkeypair \
            -keyalg RSA \
            -alias cid \
            -keystore "${keystore}" \
            -storepass "$(password)" \
            -validity 365 \
            -keysize 2048 \
            -keypass "$(password)" \
            -dname "CN=${common_name},OU=DataWorks,O=DWP,L=Leeds,ST=West Yorkshire,C=UK"
}

extract_public_certificate() {
    local keystore="${1:?Usage: $FUNCNAME keystore certificate}"
    local certificate="${2:?Usage: $FUNCNAME keystore certificate}"

    [[ -f "${certificate}" ]] && rm -v "${certificate}"

    keytool -v \
            -exportcert \
            -keystore "${keystore}" \
            -storepass "$(password)" \
            -alias cid \
            -file "$certificate"
}

make_truststore() {
    local truststore="${1:?Usage: $FUNCNAME truststore certificate}"
    local certificate="${2:?Usage: $FUNCNAME truststore certificate}"
    [[ -f ${truststore} ]] && rm -v "${truststore}"
    import_into_truststore "${truststore}" ${certificate} self
}

import_into_truststore() {
    local truststore="${1:?Usage: $FUNCNAME truststore certificate}"
    local certificate="${2:?Usage: $FUNCNAME truststore certificate}"
    local alias="${3:-cid}"

    keytool -importcert \
            -noprompt \
            -v \
            -trustcacerts \
            -alias "${alias}" \
            -file "${certificate}" \
            -keystore "${truststore}" \
            -storepass $(password)
}

password() {
    echo changeit
}

main
