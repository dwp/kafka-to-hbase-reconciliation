#!/bin/sh

set -e

# If a proxy is requested, set it up

if [ "${INTERNET_PROXY}" ]; then
    export http_proxy="http://${INTERNET_PROXY}:3128"
    export HTTP_PROXY="http://${INTERNET_PROXY}:3128"
    export https_proxy="http://${INTERNET_PROXY}:3128"
    export HTTPS_PROXY="http://${INTERNET_PROXY}:3128"
    export no_proxy=169.254.169.254,.s3.eu-west-2.amazonaws.com,s3.eu-west-2.amazonaws.com,secretsmanager.eu-west-2.amazonaws.com
    export NO_PROXY=169.254.169.254,.s3.eu-west-2.amazonaws.com,s3.eu-west-2.amazonaws.com,secretsmanager.eu-west-2.amazonaws.com
    echo "Using proxy ${INTERNET_PROXY}"
fi

exec "${@}"
