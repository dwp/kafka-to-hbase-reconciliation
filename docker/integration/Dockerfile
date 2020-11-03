FROM dwp-gradle-reconciliation:latest

COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY gradle.properties .
COPY src/ ./src
COPY build/ ./build
COPY .gradle/ ./gradle

RUN gradle :unit build integrationClasses -x test -x unit -x reconciliation-integration-test -x trim-reconciled-integration-test -x partitioned-integration-test
