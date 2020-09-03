# kafka-to-hbase-reconciliation

## Reconciliation tool to confirm that messages written from Kafka have been successfully written to HBase

This repo contains Makefile, and Dockerfile to fit the standard pattern.
This repo is a base to create new Docker image repos, adding the githooks submodule, making the repo ready for use.

After cloning this repo, please run:  
```
make bootstrap
```

## Local runs of build

Use this command to build and unit test and make a local distribution;
```
make local-all
```

## Local runs of integration tests in docker compose stack

**NOTE** depends on the local build above. This is build into the makefile commands above.

Use this command to build and unit test and make a local distribution, then run all the integration tests;
```
make integration-all
```

## Local running in an IDE

You can do this for both the application, and the integration tests.

* Make a local run configuration in i.e. Intellij from the gradle menu (see images below).
* For the app: 
  * <img src="docs/run-app-by-main.png" alt="run app" width="300"/>
  * or
  * <img src="docs/run-ide-application.png" alt="run app" width="300"/>
* For the integration tests: 
  * <img src="docs/run-ide-integration-tests" alt="run tests" width="300"/>

Either, with local application.properties

* Verify the that the [local application.properties](application.properties) matches all current docker compose settings

Or, with env vars

* Make a list duplicating the docker compose settings

Then, for either
* Edit it. 
  * Verify you have all the same settings that are used for the local docker stack.
  * Change the addresses of all hosts to `localhost` or `127.0.0.1`.
  * `HBASE_ZOOKEEPER_QUORUM=localhost`
  * `METADATASTORE_ENDPOINT=localhost`
* The ports exposed by the containers should already match.

Then for both,
* Add env vars for the logger
  * `CONTAINER_VERSION: "latest"`
  * `ENVIRONMENT: "local-dev"`
  * `APPLICATION_NAME: "reconciliation"`
  * `APP_VERSION: "test"`
  * `LOG_LEVEL: DEBUG`
* Run the application/test target you just made, it should start and connect up.

