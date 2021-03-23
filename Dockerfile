FROM openjdk:8

ARG APP_VERSION
ENV APP_NAME=reconciliation
ENV APP_JAR=$APP_NAME-$APP_VERSION.jar
ENV APP_HOME=/opt/$APP_NAME
ENV USER=reconciliation
ENV GROUP=$USER

RUN apt-get update && apt-get install uuid-runtime
RUN mkdir $APP_HOME
WORKDIR $APP_HOME

COPY entrypoint.sh .
RUN chmod a+x entrypoint.sh
COPY build/libs/*.jar ./$APP_NAME.jar

RUN mkdir -p /certs
COPY ./AmazonRootCA1.pem /certs/

RUN addgroup $GROUP
RUN useradd -g $GROUP $USER

RUN chown -R $USER.$USER . && chmod +x ./$APP_NAME.jar
RUN chown -R $GROUP:$USER /certs
RUN chmod -R a+rx /certs
RUN chmod 600 /certs/AmazonRootCA1.pem

ENV METADATA_TRUSTSTORE_CERTIFICATE=/certs/AmazonRootCA1.pem
USER $USER

ENTRYPOINT ["sh", "-c", "./entrypoint.sh \"$@\"", "--"]
