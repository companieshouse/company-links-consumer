company-links-consumer
=========================

company-links-consumer is responsible for determining if company links need to be updated

## Development
Common commands used for development and running locally can be found in the Makefile, each make target has a 
description which can be listed by running `make help`

```text
Target               Description
------               -----------
all                  Calls methods required to build a locally runnable version, typically the build target
build                Pull down any dependencies and compile code into an executable if required
clean                Reset repo to pre-build state (i.e. a clean checkout state)
deps                 Install dependencies
docker/kafka         Run kafka and create topics within docker
docker/kafka-create-topics Create kafka topics within docker
docker/kafka-start   Run kafka within docker
docker/kafka-stop    Stop kafka within docker
package              Create a single versioned deployable package (i.e. jar, zip, tar, etc.). May be dependent on the build target being run before package
sonar                Run sonar scan
test                 Run all test-* targets (convenience method for developers)
test-integration     Run integration tests
test-unit            Run unit & integration tests (see 'Makefile Changes' section below for explanation)

```
## Running kafka locally
From root folder of this project run ```docker-compose up -d```

Once containers up, run ```docker-compose exec kafka bash``` to enter kafka bash to create topics

### Create kafka topics locally
kafka-topics.sh --create   --zookeeper zookeeper:2181   --replication-factor 1 --partitions 1   --topic main-topic

kafka-topics.sh --create   --zookeeper zookeeper:2181   --replication-factor 1 --partitions 1   --topic retry-topic

kafka-topics.sh --create   --zookeeper zookeeper:2181   --replication-factor 1 --partitions 1   --topic error-topic

### Create kafka topics locally
kafka-topics.sh --list --zookeeper zookeeper:2181

### Produce kafka test messages locally
kafka-console-producer.sh --topic delta-topic --broker-list localhost:9092

## Makefile Changes
The jacoco exec file that SonarQube uses on GitHub is incomplete and, therefore, produces incorrect test coverage
reporting when code is pushed up to the repo. This is because the `analyse-pull-request` job runs when we push code to an open PR and this job runs `make test-unit`.
Therefore, the jacoco exec reporting only covers unit test coverage, not integration test coverage.

To remedy this, in the
short-term, we have decided to change the `make test-unit` command in the Makefile to run `mvn clean verify -Dskip.unit.tests=false -Dskip.integration.tests=false` instead as this
will ensure unit AND integration tests are run and that coverage is added to the jacoco reporting and, therefore, produce accurate SonarQube reporting on GitHub.

For a more in-depth explanation, please see: https://companieshouse.atlassian.net/wiki/spaces/TEAM4/pages/4357128294/DSND-1990+Tech+Debt+Spike+-+Fix+SonarQube+within+Pom+of+Projects
#
