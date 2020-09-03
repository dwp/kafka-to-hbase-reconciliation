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

1. Make a local run configuration in i.e. Intellij.
1. Edit it. 
1. Copy in all the same environment variables that are used for the local docker stack.
1. Change the addresses of all hosts to `localhost` or `127.0.0.1`
1. Change the ports to match those exposed by the containers
1. Run the application/test target you just made, it should start and connect up.
