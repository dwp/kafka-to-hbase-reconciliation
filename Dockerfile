FROM openjdk:8

ARG APP_VERSION
ENV APP_NAME=reconciliation
ENV APP_JAR=${APP_NAME}-${APP_VERSION}.jar
ENV APP_HOME=/opt/${APP_NAME}
ENV USER=reconciliation
RUN mkdir ${APP_HOME}

WORKDIR ${APP_HOME}

COPY build/libs/*.jar ./${APP_NAME}.jar

# Add Aurora cert
RUN mkdir -p /certs
COPY ./AmazonRootCA1.pem /certs/
RUN chown -R ${GROUP_NAME}:${USER_NAME} /certs
RUN chmod -R a+rx /certs
RUN chmod 600 /certs/AmazonRootCA1.pem
RUN ls -la /certs

RUN useradd ${USER} && \
        chown -R ${USER}.${USER} . && \
        chmod +x ./${APP_NAME}.jar && ls -l && pwd

USER ${USER}

ENTRYPOINT ["sh", "-c", "java -jar ./reconciliation.jar \"$@\"", "--"]
