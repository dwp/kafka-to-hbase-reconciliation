SHELL:=bash

default: help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: bootstrap
bootstrap: ## Bootstrap local environment for first use
	@make git-hooks

.PHONY: git-hooks
git-hooks: ## Set up hooks in .githooks
	@git submodule update --init .githooks ; \
	git config core.hooksPath .githooks \

build-jar: ## Build all code including tests and main jar
	gradle clean build test

dist: ## Assemble distribution files in build/dist
	gradle assembleDist

.PHONY: build-all
build-all: build-jar build-images ## Build the jar file and then all docker images

.PHONY: build-base-images
build-base-images: ## Build base images to avoid rebuilding frequently
	@{ \
		pushd resources; \
		docker build --tag dwp-centos-with-java:latest --file Dockerfile_centos_java . ; \
		docker build --tag dwp-python-preinstall:latest --file Dockerfile_python_preinstall . ; \
		popd; \
		docker build --tag dwp-gradle:latest --file resources/Dockerfile_gradle . ; \
	}

.PHONY: up-all
up-all: build-images up

up: services ## Bring up Kafka2Hbase in Docker with supporting services
	docker-compose -f docker-compose.yaml up --build -d reconciliation

restart: ## Restart Kafka2Hbase and all supporting services
	docker-compose restart

down: ## Bring down the Kafka2Hbase Docker container and support services
	docker-compose down

destroy: down ## Bring down the Kafka2Hbase Docker container and services then delete all volumes
	docker network prune -f
	docker volume prune -f

integration-test: ## Run the integration tests in a Docker container
	@{ \
		set +e ;\
		docker stop integration-test ;\
		docker rm integration-test ;\
 		set -e ;\
 	}
	docker-compose -f docker-compose.yml run --name integration-test integration-test gradle --no-daemon --rerun-tasks integration-test -x test
