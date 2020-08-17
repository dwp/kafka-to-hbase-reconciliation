FROM dwp-centos-with-java:latest

RUN mkdir -p /opt/reconciliation/data
WORKDIR /opt/reconciliation

COPY build/libs/reconciliation-0.0.1.jar ./reconciliation-latest.jar

ENTRYPOINT ["sh", "-c", "java -Dtopic_name=db.core.toDo -Dcorrelation_id=${CORRELATION_ID} -Denvironment=${ENVIRONMENT} -Dapplication=${APPLICATION} -Dapp_version=${APP_VERSION} -Dcomponent=${COMPONENT} -jar reconciliation-latest.jar \"$@\"", "--"]
