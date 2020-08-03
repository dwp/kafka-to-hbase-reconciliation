# kafka-to-hbase-reconciliation

## Reconciliation to confirm that messages written from Kafka have been successfully written to HBase

This repo contains Makefile, and Dockerfile to fit the standard pattern.
This repo is a base to create new Docker image repos, adding the githooks submodule, making the repo ready for use.

After cloning this repo, please run:  
`make bootstrap`