SHELL=bash
aws_dev_account=NOT_SET
temp_image_name=NOT_SET
aws_default_region=NOT_SET
RDBMS_READY_REGEX='mysqld: ready for connections'

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: bootstrap
bootstrap: ## Bootstrap local environment for first use
	make git-hooks

.PHONY: git-hooks
git-hooks: ## Set up hooks in .git/hooks
	@{ \
		HOOK_DIR=.git/hooks; \
		for hook in $(shell ls .githooks); do \
			if [ ! -h $${HOOK_DIR}/$${hook} -a -x $${HOOK_DIR}/$${hook} ]; then \
				mv $${HOOK_DIR}/$${hook} $${HOOK_DIR}/$${hook}.local; \
				echo "moved existing $${hook} to $${hook}.local"; \
			fi; \
			ln -s -f ../../.githooks/$${hook} $${HOOK_DIR}/$${hook}; \
		done \
	}

local-scrub: ## Scrub local output folders
	rm -rf .gradle build
	gradle clean

local-build: ## Build Kafka2HBase with gradle
	gradle :unit build -x test -x unit

local-dist: ## Assemble distribution files in build/dist with gradle
	gradle assembleDist -x test -x unit

local-test: ## Run the unit tests with gradle
	gradle --rerun-tasks unit

local-scrub-build: local-scrub local-build ## Scrub local artefacts and make new ones

local-all: local-scrub-build local-test ## local-dist ## Build and test with gradle

certificates: ## generate the mutual authentication certificates for communications with dks.
	./generate-certificates.sh

mysql-root: ## Get a root client session on the metadatastore database.
	docker exec -it metadatastore mysql --user=root --password=password metadatastore

mysql-writer: ## Get a writer client session on the metadatastore database.
	docker exec -it metadatastore mysql --user=reconciliationwriter --password=my-password metadatastore

truncate-ucfs: ## truncate the ucfs table.
	docker exec -i metadatastore \
		mysql --user=reconciliationwriter --password=my-password metadatastore <<< "truncate ucfs;"

truncate-equalities: ## truncate the ucfs table.
	docker exec -i metadatastore \
		mysql --user=reconciliationwriter --password=my-password metadatastore <<< "truncate equalities;"

truncate-hbase: ## truncate all hbase tables.
	docker exec -i hbase hbase shell <<< list \
			| egrep '^[a-z]' \
			| grep -v '^list' \
			| while read; do echo truncate \'$$REPLY\'; done \
			| docker exec -i hbase hbase shell

truncate-all: truncate-ucfs truncate-equalities truncate-hbase

hbase-shell: ## Open an HBase shell onto the running HBase container
	docker exec -it hbase hbase shell

rdbms-up: ## Bring up and provision mysql
	docker-compose -f docker-compose.yaml up -d metadatastore
	@{ \
		echo Waiting for metadatastore.; \
		while ! docker logs metadatastore 2>&1 | grep "^Version" | grep 3306; do \
			sleep 10; \
			echo Waiting for metadatastore.; \
		done; \
		echo ...metadatastore ready.; \
	}
	docker exec -i metadatastore mysql --user=root --password=password metadatastore  < ./docker/metadatastore/create_table.sql
	docker exec -i metadatastore mysql --user=root --password=password metadatastore  < ./docker/metadatastore/grant_user.sql

hbase-up: ## Bring up and provision mysql
	docker-compose -f docker-compose.yaml up -d hbase
	@{ \
		echo Waiting for hbase.; \
		while ! docker logs hbase 2>&1 | grep "Master has completed initialization" ; do \
			sleep 2; \
			echo Waiting for hbase.; \
		done; \
		echo ...hbase ready.; \
	}
	docker exec -i hbase hbase shell <<< "create_namespace 'database'"; \

dks-insecure-up: ## bring up dks on 8080
	docker-compose up -d dks-standalone-http

services: hbase-up rdbms-up dks-insecure-up ## Bring up supporting services in docker

up: services ## Bring up Reconciliation in Docker with supporting services
	docker-compose -f docker-compose.yaml up --build -d reconciliation trim-reconciled-records reconciliation-partitioned

restart: ## Restart Kafka2HBase and all supporting services
	docker-compose restart

down: ## Bring down the Kafka2HBase Docker container and support services
	docker-compose down

destroy: down ## Bring down the Kafka2HBase Docker container and services then delete all volumes
	docker network prune -f
	docker volume prune -f

integration-test-rebuild: ## Build only integration-test
	docker-compose build trim-integration-test partitioned-integration-test

trim-reconciled-integration-test: ## Run the trim reconciled integration tests in a Docker container
	docker-compose -f docker-compose.yaml up populate-for-trim
	docker-compose -f docker-compose.yaml up -d trim-reconciled-records
	docker-compose -f docker-compose.yaml up trim-integration-test
	docker-compose stop trim-reconciled-records

partitioned-integration-test: ## Run the partitioned integration tests in a Docker container
	docker-compose -f docker-compose.yaml up populate-for-partitioned
	docker-compose -f docker-compose.yaml up -d reconciliation-partitioned
	docker-compose -f docker-compose.yaml up partitioned-integration-test
	docker stop reconciliation-partitioned

integration-test-with-rebuild: integration-test-rebuild partitioned-integration-test trim-reconciled-integration-test ## Rebuild and re-run only he integration-tests

.PHONY: integration-all ## Build and Run all the tests in containers from a clean start
integration-all: destroy build services partitioned-integration-test trim-reconciled-integration-test

build: local-all build-integration-base ## build main images
	docker-compose build

build-base: ## build the base images which certain images extend.
	@{ \
		pushd docker; \
		docker build --tag dwp-java:latest --file ./java/Dockerfile . ; \
		cp ./integration_tests/shared_functions.py ./python ; \
		docker build --tag dwp-python-preinstall-reconciliation:latest --file ./python/Dockerfile . ; \
		rm python/shared_functions.py ; \
		docker build --tag dwp-python-reconciliation-integration:latest --file ./python/Dockerfile_int . ; \
		cp ../settings.gradle.kts ../gradle.properties . ; \
		docker build --tag dwp-gradle-reconciliation:latest --file ./gradle/Dockerfile . ; \
		rm -rf settings.gradle.kts gradle.properties ; \
		popd; \
	}

build-integration-base: build-base
	@{ \
		docker-compose -f docker-compose.yaml build populate-for-partitioned ; \
		docker-compose -f docker-compose.yaml build populate-for-trim ; \
		docker-compose -f docker-compose.yaml build reconciliation ; \
		docker-compose -f docker-compose.yaml build trim-reconciled-records ; \
		docker-compose -f docker-compose.yaml build trim-integration-test ; \
		docker-compose -f docker-compose.yaml build reconciliation-partitioned ; \
		docker-compose -f docker-compose.yaml build partitioned-integration-test ; \
	}

push-local-to-ecr: ## Push a temp version of reconciliation to AWS DEV ECR
	@{ \
		export AWS_DEV_ACCOUNT=$(aws_dev_account); \
		export TEMP_IMAGE_NAME=$(temp_image_name); \
		export AWS_DEFAULT_REGION=$(aws_default_region); \
		aws ecr get-login-password --region ${AWS_DEFAULT_REGION} --profile dataworks-development | docker login --username AWS --password-stdin ${AWS_DEV_ACCOUNT}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com; \
		docker tag reconciliation ${AWS_DEV_ACCOUNT}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com/${TEMP_IMAGE_NAME}; \
		docker push ${AWS_DEV_ACCOUNT}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com/${TEMP_IMAGE_NAME}; \
	}
