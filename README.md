# Apache Kafka® - Hot and Cold Retries
A demo project for elaborating how hot and cold retries can be applied in Apache Kafka®.
This repository will be the source and reference codebase of my blog post.

**It includes:**
* Application configuration and configuration class to read it
* Consumer factory to create necessary listener container beans and dead letter queue forwarder
* Simple service layer which logs the upcoming messages and to create the erroneous cases
* Topic forwarding strategies and header utilization examples in those strategies
* Consumer examples for hot and cold retries

**Utilities:**
* Also, you can find a simple Kafka cluster to create your own playground before running the application 

## How to run
The repository comes with a simple Apache Kafka® cluster utilized by `docker compose`.
As a preliminary condition, you may need to run it if there is no connectable Kafka cluster available.
Otherwise, you can configure host and port information from `application.yml`

````shell
$ cd kafka-hot-and-cold-retries
$ docker compose up # To make Kafka cluster up and running
$ gradle bootRun # To make the application up and running
````
