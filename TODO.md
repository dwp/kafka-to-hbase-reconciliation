Outstanding items


* integration tests
* fixture data for integration tests

* update readme to improve local setup
* run tests in GitHub Actions pipeline
* batch requests to HBase using existsAll() instead of exists()
* batch UPDATEs to metadata

* add retries to all connections

* run application/service in an infinite loop with interval
* query HBase replica region instead of master
* threading request to HBase and UPDATEs to metadata store
* Move SQL query date offest to variables (i.e. config and env vars)
* Use Java 14 environment in gradle build step of the pipeline
* After PR pipeline job succeeds, put the gradle build steps in the main pipeline
