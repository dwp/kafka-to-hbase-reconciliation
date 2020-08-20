# Multi stage docker build - stage 1 builds jar file
FROM dwp-kotlin-slim-gradle-reconciliation:latest as build

# Output folder
RUN mkdir -p /reconciliation_builds

# Copy the gradle config and install dependencies
COPY build.gradle.kts .

# Copy the source
COPY src/ ./src

# Create DistTar
RUN java -version
RUN gradle -version
RUN gradle :unit build -x test
RUN gradle --debug distTar
RUN find build -type f
RUN cp build/distributions/*.* /reconciliation_builds/
RUN ls -la /reconciliation_builds/

# Second build stage starts here
FROM openjdk:14-alpine

MAINTAINER DWP

COPY ./entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]
CMD ["./bin/reconciliation"]

ARG DIST_FILE=reconciliation-*.tar
ARG http_proxy_full=""

ENV APPLICATION=reconciliation
# Set user to run the process as in the docker contianer
ENV USER_NAME=reconciliation
ENV GROUP_NAME=reconciliation

# Create group and user to execute task
RUN addgroup ${GROUP_NAME}
RUN adduser --system --ingroup ${GROUP_NAME} ${USER_NAME}

# Add Aurora cert
RUN mkdir -p /certs
COPY ./AmazonRootCA1.pem /certs/
RUN chown -R ${GROUP_NAME}:${USER_NAME} /certs
RUN chmod -R a+rx /certs
RUN chmod 600 /certs/AmazonRootCA1.pem
RUN ls -la /certs

# Set environment variables for apk
ENV http_proxy=${http_proxy_full}
ENV https_proxy=${http_proxy_full}
ENV HTTP_PROXY=${http_proxy_full}
ENV HTTPS_PROXY=${http_proxy_full}

RUN echo "ENV http: ${http_proxy}" \
    && echo "ENV https: ${https_proxy}" \
    && echo "ENV HTTP: ${HTTP_PROXY}" \
    && echo "ENV HTTPS: ${HTTPS_PROXY}" \
    && echo "ARG full: ${http_proxy_full}" \
    && echo "DIST FILE: ${DIST_FILE}."

ENV acm_cert_helper_version 0.8.0
RUN echo "===> Installing Dependencies ..." \
    && echo "===> Updating base packages ..." \
    && apk update \
    && apk upgrade \
    && echo "==Update done==" \
    && apk add --no-cache util-linux \
    && echo "===> Installing acm_pca_cert_generator ..." \
    && apk add --no-cache g++ python3-dev libffi-dev openssl-dev gcc \
    && pip3 install --upgrade pip setuptools \
    && pip3 install https://github.com/dwp/acm-pca-cert-generator/releases/download/${acm_cert_helper_version}/acm_cert_helper-${acm_cert_helper_version}.tar.gz \
    && echo "==Dependencies done=="

WORKDIR /reconciliation

COPY --from=build /reconciliation_builds/$DIST_FILE .

RUN tar -xf $DIST_FILE --strip-components=1
RUN chown ${USER_NAME}:${GROUP_NAME} . -R

USER $USER_NAME
