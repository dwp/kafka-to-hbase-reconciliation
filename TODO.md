Outstanding items
* make table name variable
* source application.properties from env
* integration tests
* fixture data for integration tests
* remove gradlew
* update readme and makefile to improve local setup
* run tests in GitHub Actions pipeline
* batch requests to HBase using existsAll() instead of exists()
* batch UPDATEs to metadata
* add logging library
* add retries to all connections
* retrieve metadata store password from secrets manager (currently application.properties)
* run application/service in an infinite loop with interval
* query HBase replica region instead of master
* threading request to HBase and UPDATEs to metadata store
* Move SQL query LIMIT value and date offest to variables (i.e. config and env vars)
* Use Java 14 environment in gradle build step of the pipeline
* After PR pipeline job succeeds, put the gradle build steps in the main pipeline
